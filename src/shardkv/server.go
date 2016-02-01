package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Id           int64
	Operation    Operation
	Args         interface{}
	responseChan chan interface{}
}

func (o *Op) String() string {
	return fmt.Sprintf("id: %d, Operation: %s, Args: %#v", o.Id, o.Operation, o.Args)
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	data             [shardmaster.NShards]*Shard
	reconfigRequests map[int64]bool
	curSeq           int
	shardMasters     []string
	ownedShards      [shardmaster.NShards]bool
	opChan           chan *Op
	shardChan        chan *Shard
	closeChan        chan bool

	lastConfig atomic.Value
}

type Shard struct {
	ShardId  int
	Data     map[string]string
	Requests map[int64]bool

	syncing   bool
	ConfigNum int
}

func (kv *ShardKV) getLastConfig() *shardmaster.Config {
	return kv.lastConfig.Load().(*shardmaster.Config)
}

func (kv *ShardKV) loop() {
	for !kv.isdead() {
		select {
		case op := <-kv.opChan:
			kv.handle(op)
		case shard := <-kv.shardChan:
			if shard.ConfigNum >= kv.getLastConfig().Num {
				shard.syncing = false
				kv.data[shard.ShardId] = shard
				DPrintf("[%d:%d] sync shard %d complete @@@@@@@@\n", kv.gid, kv.me, shard.ShardId)
			} else {
				DPrintf("[%d:%d] sync shard %d complete @@@@@@@@, BUT IS TOO OLD, cur num %d, shard num %d\n", kv.gid, kv.me, shard.ShardId, kv.getLastConfig().Num, shard.ConfigNum)
			}
		case <-kv.closeChan:
			return
		}
	}
	panic("quit")
}

func (kv *ShardKV) getShardId(op *Op) int {
	switch op.Operation {
	case GET_OPERATION:
		args := op.Args.(*GetArgs)
		return key2shard(args.Key)
	case PUT_OPERATION, APPEND_OPERATION:
		args := op.Args.(*PutAppendArgs)
		return key2shard(args.Key)
	case SHARD_SYNC_OPERATION:
		args := op.Args.(*ShardSyncArgs)
		return args.ShardId
	case RECONFIG_OPERATION:
		return -1
	default:
		panic("unknown operation: " + op.Operation)
	}

}

func (kv *ShardKV) requestExisted(op *Op) bool {
	switch op.Operation {
	case GET_OPERATION:
		args := op.Args.(*GetArgs)
		shardId := key2shard(args.Key)
		if _, existed := kv.data[shardId].Requests[op.Id]; existed {
			kv.handleGet(args, op.responseChan)
			return true
		}
	case PUT_OPERATION, APPEND_OPERATION:
		args := op.Args.(*PutAppendArgs)
		shardId := key2shard(args.Key)
		if _, existed := kv.data[shardId].Requests[op.Id]; existed {
			op.responseChan <- nil
			return true
		}
	case SHARD_SYNC_OPERATION:
		args := op.Args.(*ShardSyncArgs)
		if _, existed := kv.data[args.ShardId].Requests[op.Id]; existed {
			op.responseChan <- kv.data[args.ShardId]
			return true
		}
	case RECONFIG_OPERATION:
		if _, existed := kv.reconfigRequests[op.Id]; existed {
			op.responseChan <- nil
			return true
		}
	}
	return false
}

func (kv *ShardKV) handle(op *Op) {
	DPrintf("[%d:%d] begin handle id = %d, Operation %s\n", kv.gid, kv.me, op.Id, op.Operation)
	if kv.requestExisted(op) {
		DPrintf("[%d:%d] id = %d, Operation %s, existed!!!!!!!!!!!!\n", kv.gid, kv.me, kv.curSeq, op.Id, op.Operation)
		return
	}

	var complete bool
	for !kv.isdead() && !complete {
		// DPrintf("begin handle seq %d !!\n", kv.curSeq)
		kv.px.Start(kv.curSeq, op)
		response := kv.waitSeqDecided(kv.curSeq).(*Op)
		DPrintf("[%d:%d] begin handle seq %d, id = %d, Operation %s\n", kv.gid, kv.me, kv.curSeq, response.Id, response.Operation)

		shardId := kv.getShardId(response)
		// DPrintf("begin handle seq %d, shard %d, %s\n", kv.curSeq, shardId, response)
		complete = response.Id == op.Id
		kv.handleResponse(response, op.responseChan, complete)

		// ignore reconfig operation
		if response.Operation != RECONFIG_OPERATION {
			kv.data[shardId].Requests[response.Id] = true
		} else {
			kv.reconfigRequests[response.Id] = true
		}
		kv.px.Done(kv.curSeq)
		DPrintf("[%d:%d] handle seq %d complete\n", kv.gid, kv.me, kv.curSeq)
		kv.curSeq++
	}
}

func (kv *ShardKV) waitSeqDecided(seq int) interface{} {
	to := 10 * time.Millisecond
	for {
		fate, v := kv.px.Status(seq)
		if fate == paxos.Decided {
			return v
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}

}

func (kv *ShardKV) handleResponse(op *Op, responseChan chan interface{}, complete bool) {
	switch op.Operation {
	case GET_OPERATION:
		if complete {
			kv.handleGet(op.Args.(*GetArgs), responseChan)
		}
	case SHARD_SYNC_OPERATION:
		if complete {
			kv.handleShardSync(op.Args.(*ShardSyncArgs), responseChan)
		}
	case PUT_OPERATION, APPEND_OPERATION:
		kv.handlePutAppend(op.Args.(*PutAppendArgs), responseChan, complete)
	case RECONFIG_OPERATION:
		kv.handleReconfig(op.Args.(*ReconfigArgs), responseChan, complete)
	default:
		panic("unknown operation :" + op.Operation)
	}
}

func (kv *ShardKV) handleGet(args *GetArgs, responseChan chan interface{}) {
	shardId := key2shard(args.Key)
	if !kv.ownedShards[shardId] || kv.data[shardId].syncing {
		responseChan <- ErrWrongGroup
		return
	}
	shard := kv.data[shardId]
	val, existed := shard.Data[args.Key]
	if existed {
		responseChan <- val
	} else {
		responseChan <- ErrNoKey
	}
}

func (kv *ShardKV) handlePutAppend(args *PutAppendArgs, responseChan chan interface{}, complete bool) {
	shardId := key2shard(args.Key)
	if !kv.ownedShards[shardId] {
		if complete {
			responseChan <- ErrWrongGroup
		}
		return
	}
	shard := kv.data[shardId]
	val, existed := shard.Data[args.Key]
	if !existed || args.Op == "PUT" {
		shard.Data[args.Key] = args.Value
	} else {
		shard.Data[args.Key] = val + args.Value
	}
	if complete {
		responseChan <- nil
	}
}

func (kv *ShardKV) handleShardSync(args *ShardSyncArgs, responseChan chan interface{}) {
	// if kv.data[args.ShardId].syncing {
	//	DPrintf("[%d:%d] shardid %d owned %v, syncing %v @@@@@@@@@\n", kv.gid, kv.me, args.ShardId, kv.ownedShards[args.ShardId], kv.data[args.ShardId].syncing)
	//	responseChan <- ErrWrongGroup
	//	return
	// }
	shard := kv.data[args.ShardId]
	copiedShardData := make(map[string]string)
	copiedShardRequests := make(map[int64]bool)
	for key, val := range shard.Data {
		copiedShardData[key] = val
	}
	for key, val := range shard.Requests {
		copiedShardRequests[key] = val
	}
	copiedShard := &Shard{
		ShardId:   args.ShardId,
		Requests:  copiedShardRequests,
		Data:      copiedShardData,
		ConfigNum: kv.getLastConfig().Num,
	}
	DPrintf("handle shard  %d sync complete@@@@@@@@@@@@@@\n", args.ShardId)
	responseChan <- copiedShard
}

func (kv *ShardKV) handleReconfig(args *ReconfigArgs, responseChan chan interface{}, complete bool) {
	config := args.Config
	lastConfig := kv.lastConfig.Load().(*shardmaster.Config)
	if lastConfig.Num >= config.Num {
		return
	}
	DPrintf("[%d:%d] Reconfig num %d begin!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n", kv.gid, kv.me, config.Num)

	var newShards [shardmaster.NShards]bool
	for shardId, gid := range config.Shards {
		if gid == kv.gid {
			newShards[shardId] = true
		}
	}

	shardFunc := func(shardId int, gid int64) {
		DPrintf("[%d:%d] begin request shardid %d @@@@@@@@@\n", kv.gid, kv.me, shardId)
		clerk := MakeClerk(kv.shardMasters)
		shard := clerk.ShardSync(shardId, gid)
		DPrintf("[%d:%d] begin request shardid %d COMMMPLETE @@@@@@@@@\n", kv.gid, kv.me, shardId)
		kv.shardChan <- shard
	}

	var counter int
	// if owned a new shard
	for shardId, owned := range newShards {
		// some one owned shard before, request it
		if owned && !kv.ownedShards[shardId] && lastConfig.Shards[shardId] != 0 {
			kv.data[shardId].syncing = true
			go shardFunc(shardId, lastConfig.Shards[shardId])
			counter++
		}
	}

	kv.ownedShards = newShards
	if lastConfig != nil {
		DPrintf("[%d:%d] Reconfig num %d -> %d complete\n", kv.gid, kv.me, lastConfig.Num, config.Num)
	} else {
		DPrintf("[%d:%d] Reconfig num %d complete\n", kv.gid, kv.me, config.Num)
	}
	kv.lastConfig.Store(config)
	if complete {
		responseChan <- nil
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	op := &Op{
		Id:           args.Id,
		Operation:    GET_OPERATION,
		Args:         args,
		responseChan: make(chan interface{}),
	}
	DPrintf("[%d:%d] get key %s\n", kv.gid, kv.me, args.Key)
	kv.opChan <- op
	DPrintf("get key %s xxxxxx\n", args.Key)
	val := <-op.responseChan
	DPrintf("get key %s, response %s\n", args.Key, val)
	if err, ok := val.(Err); ok {
		reply.Err = err
	} else {
		reply.Value = val.(string)
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	var operation Operation
	if args.Op == "Put" {
		operation = PUT_OPERATION
	} else {
		operation = APPEND_OPERATION
	}
	DPrintf("me %d, gid %d, begin put\n", kv.me, kv.gid)
	op := &Op{
		Id:           args.Id,
		Operation:    operation,
		Args:         args,
		responseChan: make(chan interface{}),
	}
	kv.opChan <- op
	val := <-op.responseChan
	DPrintf("me %d, gid %d, receive response\n", kv.me, kv.gid)
	if err, ok := val.(Err); ok {
		reply.Err = err
	}
	return nil
}

func (kv *ShardKV) ShardSync(args *ShardSyncArgs, reply *ShardSyncReply) error {
	op := &Op{
		Id:           args.Id,
		Operation:    SHARD_SYNC_OPERATION,
		Args:         args,
		responseChan: make(chan interface{}),
	}
	kv.opChan <- op
	response := <-op.responseChan
	if err, ok := response.(Err); ok {
		reply.Err = err
	} else {
		reply.Shard = response.(*Shard)
	}
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	config := kv.sm.Query(-1)
	lastConfig := kv.lastConfig.Load().(*shardmaster.Config)
	if lastConfig.Num == config.Num {
		return
	}

	DPrintf("[%d:%d] TICK last num %d:, cur config num %d\n", kv.gid, kv.me, lastConfig.Num, config.Num)
	op := &Op{
		Id:           int64(config.Num),
		Operation:    RECONFIG_OPERATION,
		Args:         &ReconfigArgs{Config: &config},
		responseChan: make(chan interface{}),
	}
	kv.opChan <- op
	<-op.responseChan
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	close(kv.closeChan)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(&Op{})
	gob.Register(&PutAppendArgs{})
	gob.Register(&GetArgs{})
	gob.Register(&ReconfigArgs{})
	gob.Register(&ShardSyncArgs{})
	gob.Register(&Shard{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.lastConfig.Store(&shardmaster.Config{Num: -1})
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.shardMasters = shardmasters
	kv.reconfigRequests = make(map[int64]bool)
	kv.shardChan = make(chan *Shard)
	kv.opChan = make(chan *Op)
	kv.closeChan = make(chan bool)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.data[i] = &Shard{
			ShardId:  i,
			Requests: make(map[int64]bool),
			Data:     make(map[string]string),
		}
	}

	// Your initialization code here.
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	go kv.loop()

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
