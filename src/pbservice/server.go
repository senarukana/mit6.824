package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	state       State
	view        viewservice.View
	requestChan chan *OperationArgs
	data        map[string]*Value

	// replication
	replication    *Replication
	replChan       chan *RepliOperationArgs
	backupAddress  string
	primaryAddress string
}

type Value struct {
	token int64
	data  string
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.Lock()
	defer pb.Unlock()

	if pb.state != PRIMARY {
		reply.Err = Err(fmt.Sprintf("plz send get command to primary, my state is %v", pb.state))
	} else {
		if _, ok := pb.data[args.Key]; ok {
			reply.Value = pb.data[args.Key].data
		}
	}

	// resultChan := make(chan interface{})
	// pb.requestChan <- &OperationArgs{
	//	op:         GET_OPERATION,
	//	args:       args,
	//	resultChan: resultChan,
	// }
	// *reply = *((<-resultChan).(*GetReply))
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.Lock()
	if pb.state == IDLE {
		reply.Err = "plz send command to others, i'm a idle server"
	} else {
		// log.Printf("[%s] Append %s, value: %s\n", pb.me, args.Key, args.Value)
		item, existed := pb.data[args.Key]
		// avoid duplication
		if existed {
			if item.token == args.Token {
				pb.Unlock()
				return nil
			}
		}
		value := &Value{
			token: args.Token,
		}
		if args.Operation == PUT_OPERATION || !existed {
			value.data = args.Value
			pb.data[args.Key] = value
		} else {
			value.data = item.data + args.Value
			pb.data[args.Key] = value
		}
		if pb.replChan != nil {
			pb.Unlock()
			doneChan := make(chan bool)
			pb.replChan <- &RepliOperationArgs{
				PutAppendArgs: args,
				doneChan:      doneChan,
			}
			<-doneChan
			return nil
		}
	}
	pb.Unlock()
	return nil

	// resultChan := make(chan interface{})
	// pb.requestChan <- &OperationArgs{
	//	op:         GET_OPERATION,
	//	args:       args,
	//	resultChan: resultChan,
	// }
	// *reply = *((<-resultChan).(*PutAppendReply))
	// return nil
}

// func (pb *PBServer) Loop() {
//	tickTick := time.Tick(viewservice.PingInterval)

//	for !pb.isdead() {
//		select {
//		case request := <-pb.requestChan:
//			switch request.op {
//			case GET_OPERATION:
//				pb.get(request)
//			case PUT_OPERATION, APPEND_OPERATION:
//				pb.putAppend(request)
//			default:
//				panic(fmt.Sprintf("unknown operation: %v", request.op))
//			}
//		case <-tickTick:
//			pb.tick()
//		}
//	}

// }

// func (pb *PBServer) Get(args *OperationArgs) {
//	reply := &GetReply{}
//	getArgs := args.args.(*GetArgs)
//	if pb.state != PRIMARY {
//		reply.Err = Err(fmt.Sprintf("plz send get command to primary, my state is %v", pb.state))
//	} else {
//		reply.Value = pb.data[getArgs.Key]
//	}
// }

// func (pb *PBServer) putAppend(args *OperationArgs) {
//	reply := &PutAppendReply{}
//	putAppendArgs := args.args.(*PutAppendArgs)
//	if pb.state == IDLE {
//		reply.Err = "plz send command to others, i'm a idle server"
//	} else {
//		if putAppendArgs.Operation == PUT_OPERATION {
//			pb.data[putAppendArgs.Key] = putAppendArgs.Value
//		} else {
//			pb.data[putAppendArgs.Key] += putAppendArgs.Value
//		}
//		if pb.replChan != nil {
//			doneChan := make(chan bool)
//			pb.replChan <- &RepliOperationArgs{
//				PutAppendArgs: putAppendArgs,
//				doneChan:      doneChan,
//			}
//			<-doneChan
//		}
//	}
// }
func (pb *PBServer) clear() {
	pb.state = IDLE
	pb.data = make(map[string]*Value)
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.Lock()
	defer pb.Unlock()
	view, err := pb.vs.Ping(pb.view.Viewnum)
	if err != nil {
		log.Printf("ping view server error: %s", err)
		return
	}
	if view.Viewnum == pb.view.Viewnum {
		return
	}
	shouldAck := true
	if pb.state == IDLE {
		// idle -> primary, this will only happen at beginning
		if view.Primary == pb.me {
			pb.state = PRIMARY
		} else if view.Backup == pb.me { // idle -> backup
			pb.state = BACKUP
		} else { // primary or backup changed, but not me
		}
	} else if pb.state == BACKUP {
		// backup -> primary
		if view.Primary == pb.me {
			pb.state = PRIMARY
			pb.backupAddress = view.Backup
			log.Printf("[%s] become the primary, data size %d\n", pb.me, len(pb.data))
			pb.replChan = pb.replication.start(pb.data, view.Backup)
			// we shouldn't ack received view num as backup is still in progress
			if view.Backup != "" {
				shouldAck = false
			}
		} else if view.Backup != pb.me { // backup -> idle
			pb.clear()
		} else { // primary changed, this shouldn't happen!!
			panic("I was a backup, but primary has changed!")
		}
	} else {
		if view.Backup == pb.me { // primary -> backup
			pb.state = BACKUP
			pb.replication.stop()
			pb.replChan = nil
		} else if view.Primary == pb.me { // backup in progress or backup changed
			if view.Backup == pb.backupAddress {
				log.Printf("me %s, backup %s synced %v", pb.me, pb.backupAddress, pb.replication.synced)
				if !pb.replication.synced {
					shouldAck = false
				}
			} else {
				pb.backupAddress = view.Backup
				pb.replChan = pb.replication.start(pb.data, view.Backup)
				if view.Backup != "" {
					shouldAck = false
				}
			}
		} else { // primary -> idle
			pb.clear()
		}
	}
	if shouldAck {
		pb.view = view
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.requestChan = make(chan *OperationArgs)
	pb.replication = NewReplication(me)
	pb.data = make(map[string]*Value)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()
	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()
	return pb
}
