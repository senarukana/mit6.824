package kvpaxos

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

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id           int64
	Operation    Operation
	Key          string
	Value        string
	responseChan chan interface{}
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos
	// Your definitions here.
	data      map[string]string
	opChan    chan *Op
	closeChan chan bool
	curSeq    int
}

func (kv *KVPaxos) loop() {
	for !kv.isdead() {
		select {
		case op := <-kv.opChan:
			kv.handleOP(op)
		case <-kv.closeChan:
			return
		}
	}
}

func (kv *KVPaxos) handleOP(op *Op) {
	var complete bool
	for !kv.isdead() && !complete {
		kv.px.Start(kv.curSeq, *op)
		response := kv.waitSeqDecided(kv.curSeq).(Op)
		complete = response.Id == op.Id
		kv.handle(&response, op.responseChan, complete)
		kv.curSeq++
	}
	log.Printf("handle op key %s complete\n", op.Key)
}

func (kv *KVPaxos) waitSeqDecided(seq int) interface{} {
	log.Printf("wait seq %d decided\n", seq)
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

func (kv *KVPaxos) handle(op *Op, responseChan chan interface{}, complete bool) {
	if op.Operation == GET_OPERATION {
		if complete {
			if v, existed := kv.data[op.Key]; existed {
				responseChan <- v
			} else {
				responseChan <- ErrNoKey
			}
		}
	} else {
		if op.Operation == PUT_OPERATION {
			kv.data[op.Key] = op.Value
		} else {
			kv.data[op.Key] += op.Value
		}
		log.Printf("put key %s, val %s, data %s\n", op.Key, op.Value, kv.data[op.Key])
		if complete {
			responseChan <- nil
		}
	}

}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	op := &Op{
		Id:           nrand(),
		Key:          args.Key,
		Operation:    GET_OPERATION,
		responseChan: make(chan interface{}),
	}
	kv.opChan <- op
	val := <-op.responseChan
	if err, ok := val.(Err); ok {
		reply.Err = err
	} else {
		reply.Value = val.(string)
	}
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	var operation Operation
	if args.Op == "Put" {
		operation = PUT_OPERATION
	} else {
		operation = APPEND_OPERATION
	}
	op := &Op{
		Id:           nrand(),
		Key:          args.Key,
		Value:        args.Value,
		Operation:    operation,
		responseChan: make(chan interface{}),
	}
	kv.opChan <- op
	<-op.responseChan
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	close(kv.closeChan)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.data = make(map[string]string)
	kv.opChan = make(chan *Op)
	kv.closeChan = make(chan bool)
	go kv.loop()

	// Your initialization code here.

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
