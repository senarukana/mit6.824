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
	state    int32
	curTick  int64
	lastTick int64

	// view
	lock sync.Mutex // protect view
	view *viewservice.View

	// data
	data map[string]*Value
}

type Value struct {
	ClientHistory map[int64]int64
	Data          string
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.lock.Lock()
	defer pb.lock.Unlock()
	if pb.state != PRIMARY {
		reply.Err = Err(fmt.Sprintf("plz send get command to primary, my state is %v", atomic.LoadInt32(&pb.state)))
	} else {
		if _, ok := pb.data[args.Key]; ok {
			reply.Value = pb.data[args.Key].Data
		}
	}
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.lock.Lock()
	defer pb.lock.Unlock()
	if pb.state == IDLE {
		reply.Err = "plz send PutAppend command to others, i'm a idle server"
		return nil
	}
	if pb.state == PRIMARY && pb.view != nil && pb.view.Backup != "" {
		var reply PutAppendReply
		ok := call(pb.view.Backup, "PBServer.PutAppend", args, &reply)
		if !ok || reply.Err != "" {
			return fmt.Errorf("call server %s error", pb.view.Backup)
		}
	}

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

func (pb *PBServer) Sync(args *SyncArgs, reply *SyncReply) error {
	pb.lock.Lock()
	defer pb.lock.Unlock()
	if pb.state != BACKUP {
		reply.Err = Err(fmt.Sprintf("plz send Replicate command to others, i'm %s not a backup server %d", pb.me, atomic.LoadInt32(&pb.state)))
	} else {
		pb.data = args.Data
	}
	return nil
}

// assume sync will hold lock before hand
func (pb *PBServer) sync(view *viewservice.View) bool {
	if view.Backup == "" {
		return true
	}
	data := make(map[string]*Value)
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
	args := &SyncArgs{
		Data: data,
	}
	var reply PutAppendReply
	ok := call(view.Backup, "PBServer.Sync", args, &reply)
	if ok && reply.Err == "" {
		return true
	} else {
		return false
	}
}

// assume clear will hold lock before hand
func (pb *PBServer) clear() {
	pb.data = make(map[string]*Value)
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.lock.Lock()
	defer pb.lock.Unlock()
	pb.curTick++

	view, err := pb.vs.Ping(pb.view.Viewnum)
	if err != nil {
		log.Printf("ping view server error: %s", err)
		// give up
		if pb.curTick-pb.lastTick >= viewservice.DeadPings {
			pb.clear()
			pb.state = IDLE
			pb.view.Viewnum = 0
		}
		return
	}
	pb.lastTick = pb.curTick
	if pb.view != nil && view.Viewnum == pb.view.Viewnum {
		return
	}

	if view.Primary == pb.me {
		if !pb.sync(&view) {
			return
		}
		pb.state = PRIMARY
	} else if view.Backup == pb.me {
		if pb.state == PRIMARY {
			pb.clear()
		}
		pb.state = BACKUP
	} else {
		if pb.state != IDLE {
			pb.clear()
		}
		pb.state = IDLE
	}
	pb.view = &view
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
	pb.view = &viewservice.View{}

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
