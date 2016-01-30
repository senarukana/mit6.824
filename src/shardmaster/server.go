package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "container/heap"
import "reflect"
import crand "crypto/rand"
import "math/big"

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	opChan    chan *Op
	configs   []Config // indexed by config num
	curSeq    int
	closeChan chan bool
}

type groupCounter struct {
	gid    int64
	shards int
}

type groupCounters []*groupCounter

func (a *groupCounters) Push(x interface{}) {
	*a = append(*a, x.(*groupCounter))
}

func (a *groupCounters) Pop() interface{} {
	old := *a
	n := len(old)
	ret := old[n-1]
	*a = old[:n-1]
	return ret
}
func (a groupCounters) Len() int { return len(a) }
func (a groupCounters) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a groupCounters) Less(i, j int) bool {
	if a[i].shards != a[j].shards {
		return a[i].shards < a[j].shards
	}
	return a[i].gid < a[j].gid
}

type Op struct {
	Operation    Operation
	Id           int64
	Args         interface{}
	responseChan chan interface{}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	responseChan := make(chan interface{})
	sm.opChan <- &Op{
		Operation:    JOIN_OPERATION,
		Args:         args,
		Id:           nrand(),
		responseChan: responseChan,
	}
	<-responseChan
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	responseChan := make(chan interface{})
	sm.opChan <- &Op{
		Operation:    LEAVE_OPERATION,
		Args:         args,
		Id:           nrand(),
		responseChan: responseChan,
	}
	<-responseChan
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	responseChan := make(chan interface{})
	sm.opChan <- &Op{
		Operation:    MOVE_OPERATION,
		Args:         args,
		Id:           nrand(),
		responseChan: responseChan,
	}
	<-responseChan
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	responseChan := make(chan interface{})
	sm.opChan <- &Op{
		Operation:    QUERY_OPERATION,
		Args:         args,
		responseChan: responseChan,
		Id:           nrand(),
	}
	reply.Config = (<-responseChan).(Config)
	return nil
}

func (sm *ShardMaster) loop() {
	for {
		select {
		case op := <-sm.opChan:
			sm.handle(op)
		case <-sm.closeChan:
			return
		}
	}
}

func (sm *ShardMaster) handle(op *Op) {
	var complete bool
	for !sm.isdead() && !complete {
		sm.px.Start(sm.curSeq, op)
		response := sm.waitSeqDecided(sm.curSeq).(*Op)
		complete = response.Id == op.Id
		sm.handleResponse(response, op.responseChan, complete)
		sm.px.Done(sm.curSeq)
		sm.curSeq++
	}
}

func (sm *ShardMaster) waitSeqDecided(seq int) interface{} {
	to := 10 * time.Millisecond
	for {
		fate, v := sm.px.Status(seq)
		if fate == paxos.Decided {
			return v
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}

}

func (sm *ShardMaster) handleResponse(op *Op, responseChan chan interface{}, complete bool) {
	switch op.Operation {
	case QUERY_OPERATION:
		if complete {
			sm.handleQuery(op.Args.(*QueryArgs), responseChan)
		}
		return
	case JOIN_OPERATION:
		sm.handleJoin(op.Args.(*JoinArgs))
	case LEAVE_OPERATION:
		sm.handleLeave(op.Args.(*LeaveArgs))
	case MOVE_OPERATION:
		sm.handleMove(op.Args.(*MoveArgs))
	}
	if complete {
		responseChan <- nil
	}
}

func (sm *ShardMaster) handleQuery(args *QueryArgs, responseChan chan interface{}) {
	if args.Num == -1 || args.Num >= len(sm.configs) {
		responseChan <- sm.configs[len(sm.configs)-1]
	} else {
		responseChan <- sm.configs[args.Num]
	}

}

func createCounters(config *Config, asce bool) groupCounters {
	var gs groupCounters
	counts := make(map[int64]int)
	for gid, _ := range config.Groups {
		counts[gid] = 0
	}
	for _, gid := range config.Shards {
		if asce {
			counts[gid]++
		} else {
			counts[gid]--
		}
	}

	for gid, shards := range counts {
		gs = append(gs, &groupCounter{
			gid:    gid,
			shards: shards,
		})
	}
	heap.Init(&gs)
	return gs
}

func (sm *ShardMaster) handleJoin(args *JoinArgs) {
	if len(sm.configs) == 1 {
		groups := make(map[int64][]string)
		groups[args.GID] = args.Servers
		var shards [NShards]int64
		for i := 0; i < NShards; i++ {
			shards[i] = args.GID
		}
		config := Config{
			Num:    1,
			Groups: groups,
			Shards: shards,
		}
		sm.configs = append(sm.configs, config)
	} else {
		lastConfig := sm.configs[len(sm.configs)-1]
		// check if has server groups before
		if groupServers, existed := lastConfig.Groups[args.GID]; existed {
			if !reflect.DeepEqual(groupServers, args.Servers) {
				// may be have a new config?
				lastConfig.Groups[args.GID] = args.Servers
			}
			return
		}
		newGroups := make(map[int64][]string)
		for key, value := range lastConfig.Groups {
			newGroups[key] = make([]string, len(value))
			copy(newGroups[key], value)
		}
		newGroups[args.GID] = args.Servers
		newShards := lastConfig.Shards

		counters := createCounters(&lastConfig, false)
		desired := NShards / len(newGroups)
		for desired > 0 {
			gc := heap.Pop(&counters).(*groupCounter)
			for i := 0; i < NShards; i++ {
				if newShards[i] == gc.gid {
					newShards[i] = args.GID
					break
				}
			}
			// counters is sorted by desc, shards is negative
			gc.shards++
			counters.Push(gc)
			desired--
		}

		newConfig := Config{
			Num:    lastConfig.Num + 1,
			Groups: newGroups,
			Shards: newShards,
		}
		// log.Printf("after join op %d\n", args.GID)
		// for i := 0; i < NShards; i++ {
		//	log.Printf("shard %d -> %d\n", i, newShards[i])
		// }
		sm.configs = append(sm.configs, newConfig)
	}
}

func (sm *ShardMaster) handleLeave(args *LeaveArgs) {
	lastConfig := sm.configs[len(sm.configs)-1]
	// check if config has server groups
	if _, existed := lastConfig.Groups[args.GID]; !existed {
		return
	}
	newConfig := Config{
		Num: lastConfig.Num + 1,
	}
	newGroups := make(map[int64][]string)
	if len(lastConfig.Groups) == 1 {
		newConfig.Groups = newGroups
	} else {
		for key, value := range lastConfig.Groups {
			if key != args.GID {
				newGroups[key] = make([]string, len(value))
				copy(newGroups[key], value)
			}
		}
		newShards := lastConfig.Shards

		counters := createCounters(&lastConfig, true)
		var desired int
		for i, counter := range counters {
			if counter.gid == args.GID {
				desired = counter.shards
				counters = append(counters[:i], counters[i+1:]...)
				break
			}
		}
		for desired > 0 {
			gc := heap.Pop(&counters).(*groupCounter)
			for i := 0; i < NShards; i++ {
				if newShards[i] == args.GID {
					newShards[i] = gc.gid
					break
				}
			}
			gc.shards++
			counters.Push(gc)
			desired--
		}

		newConfig.Groups = newGroups
		newConfig.Shards = newShards
		// log.Printf("after leave op %d\n", args.GID)
		// for i := 0; i < NShards; i++ {
		//	log.Printf("shard %d -> %d\n", i, newShards[i])
		// }
		// for gid, servers := range newGroups {
		//	log.Printf("gid %d, servers %#v\n", gid, servers)
		// }
	}
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) handleMove(args *MoveArgs) {
	lastConfig := sm.configs[len(sm.configs)-1]
	// check if config has server groups
	if _, existed := lastConfig.Groups[args.GID]; !existed {
		return
	}
	newGroups := make(map[int64][]string)
	for key, value := range lastConfig.Groups {
		newGroups[key] = make([]string, len(value))
		copy(newGroups[key], value)
	}
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Groups: newGroups,
		Shards: lastConfig.Shards,
	}
	newConfig.Shards[args.Shard] = args.GID
	sm.configs = append(sm.configs, newConfig)

}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.opChan = make(chan *Op)

	rpcs := rpc.NewServer()

	gob.Register(&Op{})
	gob.Register(&JoinArgs{})
	gob.Register(&LeaveArgs{})
	gob.Register(&MoveArgs{})
	gob.Register(&QueryArgs{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	go sm.loop()

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
