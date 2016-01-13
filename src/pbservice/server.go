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
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.

	// state
	state       int32
	replicating int32
	curTick     int64
	lastTick    int64

	// view
	lock     sync.Mutex // protect view
	view     *viewservice.View
	lastView *viewservice.View

	// data
	writeCond *sync.Cond // protect data
	data      map[string]*Value
}

type Value struct {
	ClientHistory map[int64]int64
	Data          string
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	if atomic.LoadInt32(&pb.state) != PRIMARY {
		reply.Err = Err(fmt.Sprintf("plz send get command to primary, my state is %v", atomic.LoadInt32(&pb.state)))
	} else {
		pb.writeCond.L.Lock()
		if _, ok := pb.data[args.Key]; ok {
			reply.Value = pb.data[args.Key].Data
		}
		pb.writeCond.L.Unlock()
	}
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	if atomic.LoadInt32(&pb.state) == IDLE {
		reply.Err = "plz send PutAppend command to others, i'm a idle server"
		return nil
	}
	pb.writeCond.L.Lock()
	for atomic.LoadInt32(&pb.replicating) != 0 {
		pb.writeCond.Wait()
	}
	// log.Printf("[%s] Append %s, value: %s\n", pb.me, args.Key, args.Value)
	// replicate to backup

	pb.lock.Lock()
	needReplicate := atomic.LoadInt32(&pb.state) == PRIMARY && pb.view != nil && pb.view.Backup != ""
	if needReplicate {
		pb.writeCond.L.Unlock()
		atomic.StoreInt32(&pb.replicating, 1)
		defer pb.writeCond.Signal()
		backup := pb.view.Backup
		var reply PutAppendReply
		pb.lock.Unlock()
		err := pb.replicateCall(backup, "PBServer.PutAppend", args, &reply)
		atomic.StoreInt32(&pb.replicating, 0)
		if err != nil {
			return err
		}
		pb.writeCond.L.Lock()
	} else {
		pb.lock.Unlock()
	}
	defer pb.writeCond.L.Unlock()

	val, existed := pb.data[args.Key]
	// avoid duplication
	if existed {
		token, clientExisted := val.ClientHistory[args.ClientIdentifier]
		if clientExisted && token == args.Token {
			return nil
		}
	} else {
		val = &Value{ClientHistory: make(map[int64]int64)}
	}
	val.ClientHistory[args.ClientIdentifier] = args.Token
	if args.Operation == PUT_OPERATION || !existed {
		val.Data = args.Value
		pb.data[args.Key] = val
	} else {
		val.Data = val.Data + args.Value
		pb.data[args.Key] = val
	}
	return nil
}

func (pb *PBServer) replicateCall(backup string, rpcname string, args interface{}, reply *PutAppendReply) error {
	for {
		ok := call(backup, rpcname, args, reply)
		if ok && reply.Err == "" {
			break
		}
		pb.lock.Lock()
		changed := atomic.LoadInt32(&pb.state) != PRIMARY || (pb.lastView != nil && pb.lastView.Backup != backup)
		pb.lock.Unlock()
		if changed {
			// log.Println("view has changed, give up replicate call")
			return fmt.Errorf("give up replicate call %s, view has changed", backup)
		}
		reply.Err = ""
		time.Sleep(viewservice.PingInterval)
	}
	return nil
}

func (pb *PBServer) Sync(args *ReplicateArgs, reply *PutAppendReply) error {
	if atomic.LoadInt32(&pb.state) != BACKUP {
		reply.Err = Err(fmt.Sprintf("plz send Replicate command to others, i'm %s not a backup server %d", pb.me, atomic.LoadInt32(&pb.state)))
	} else {
		pb.writeCond.L.Lock()
		pb.data = args.Data
		pb.writeCond.L.Unlock()
	}
	return nil
}

func (pb *PBServer) sync(view *viewservice.View) bool {
	data := make(map[string]*Value)
	pb.writeCond.L.Lock()
	atomic.StoreInt32(&pb.replicating, 1)
	for key, value := range pb.data {
		copiedValue := &Value{
			Data:          value.Data,
			ClientHistory: make(map[int64]int64),
		}
		for client, token := range value.ClientHistory {
			copiedValue.ClientHistory[client] = token
		}
		data[key] = copiedValue
	}
	pb.writeCond.L.Unlock()

	args := &ReplicateArgs{
		Data: data,
	}
	var reply PutAppendReply
	err := pb.replicateCall(view.Backup, "PBServer.Sync", args, &reply)
	atomic.StoreInt32(&pb.replicating, 0)
	pb.writeCond.Signal()
	if err == nil {
		pb.lock.Lock()
		pb.view = view
		pb.lock.Unlock()
		return true
	}
	return false
}

func (pb *PBServer) clear() {
	pb.writeCond.L.Lock()
	pb.data = make(map[string]*Value)
	pb.writeCond.L.Unlock()
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	var viewNum uint = 0
	atomic.AddInt64(&pb.curTick, 1)

	pb.lock.Lock()
	if pb.view != nil {
		viewNum = pb.view.Viewnum
	}
	pb.lock.Unlock()

	view, err := pb.vs.Ping(viewNum)
	if err != nil {
		log.Printf("ping view server error: %s", err)
		// give up
		if pb.curTick-pb.lastTick >= viewservice.DeadPings {
			atomic.StoreInt32(&pb.state, IDLE)
		}
		return
	}
	atomic.StoreInt64(&pb.lastTick, pb.curTick)
	pb.lock.Lock()
	// log.Printf("[%s] tick state %d, view primary %s, backup %s\n", pb.me, atomic.LoadInt32(&pb.state), view.Primary, view.Backup)
	notChanged := ((pb.view != nil && view.Viewnum == pb.view.Viewnum) || (pb.lastView != nil && view.Viewnum == pb.lastView.Viewnum))
	pb.lock.Unlock()

	if notChanged {
		return
	}

	if view.Primary == pb.me {
		atomic.StoreInt32(&pb.state, PRIMARY)
		if view.Backup != "" {
			go pb.sync(&view)
		}
		pb.lock.Lock()
		pb.lastView = &view
		// no need to sync
		if view.Backup == "" {
			pb.view = &view
		}
		pb.lock.Unlock()
		return
	} else if view.Backup == pb.me {
		if atomic.LoadInt32(&pb.state) == PRIMARY {
			pb.clear()
		}
		atomic.StoreInt32(&pb.state, BACKUP)
	} else {
		if atomic.LoadInt32(&pb.state) != IDLE {
			pb.clear()
		}
		atomic.StoreInt32(&pb.state, IDLE)
	}
	pb.lock.Lock()
	pb.view = &view
	pb.lastView = &view
	pb.lock.Unlock()
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
	pb.data = make(map[string]*Value)
	pb.writeCond = sync.NewCond(&sync.Mutex{})

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
