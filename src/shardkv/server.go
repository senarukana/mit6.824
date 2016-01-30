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

const Debug = 0

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
	data        map[int]map[string]string
	requests    map[int64]bool
	config      *shardmaster.Config
	ownedShards map[int]bool
	opChan      chan *Op
	closeChan   chan bool
	curSeq      int
}

func (kv *ShardKV) loop() {
	for !kv.isdead() {
		select {
		case op := <-kv.opChan:
			kv.handle(op)
		case <-kv.closeChan:
			return
		}
	}
}

func (kv *ShardKV) handle(op *Op) {
	var complete bool
	if _, existed := kv.requests[op.Id]; existed {
		if op.Operation == GET_OPERATION {
			kv.handleGet(op.Args.(*GetArgs), op.responseChan)
		} else {
			op.responseChan <- nil
		}
		return
	}
	for !kv.isdead() && !complete {
		kv.px.Start(kv.curSeq, op)
		response := kv.waitSeqDecided(kv.curSeq).(*Op)
		complete = response.Id == op.Id
		kv.handleResponse(response, op.responseChan, complete)
		kv.requests[response.Id] = true
		kv.px.Done(kv.curSeq)
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
		return
	case PUT_APPEND_OPERATION:
		kv.handlePutAppend(op.Args.(*PutAppendArgs), responseChan, complete)
	case SHARD_SYNC_OPERATION:
		kv.handleShardSync(op.Args.(*ShardSyncArgs), responseChan, complete)
	case RECONFIG_OPERATION:
		// sm.handleMove(op.Args.(*MoveArgs))
	}
}

func (kv *ShardKV) handleGet(args *GetArgs, responseChan chan interface{}) {
	shardId := key2shard(args.Key)
	if !kv.ownedShards[shardId] {
		responseChan <- ErrWrongGroup
	}
	shard := kv.data[shardId]
	val, existed := shard[args.Key]
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
	val, existed := shard[args.Key]
	if !existed {
		shard[args.Key] = args.Value
	} else {
		if args.Op == "PUT" {
			shard[args.Key] = args.Value
		} else {
			shard[args.Key] = val + args.Value
		}
	}
	if complete {
		responseChan <- nil
	}
}

func (kv *ShardKV) handleShardSync(args *ShardSyncArgs, responseChan chan interface{}, complete bool) {
	if !kv.ownedShards[args.ShardId] {
		if complete {
			responseChan <- ErrWrongGroup
		}
	}
	shard := kv.data[args.ShardId]
	copiedShard := make(map[string]string)
	for key, val := range shard {
		copiedShard[key] = val
	}
	responseChan <- copiedShard
}

func (kv *ShardKV) handleReconfig(args *ReconfigArgs, responseChan chan interface{}, complete bool) {

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
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
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

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
