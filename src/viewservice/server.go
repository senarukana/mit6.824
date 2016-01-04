package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	view        View
	needAcked   bool
	curTick     int
	primaryTick int
	backupTick  int
	// prevent no backup server situation
	idleTick   int
	idleServer string
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.Lock()
	defer vs.Unlock()
	changed := false

	defer func() error {
		if changed {
			vs.view.Viewnum++
			vs.needAcked = true
		}
		if args.Me == vs.view.Primary {
			vs.primaryTick = vs.curTick
		} else if args.Me == vs.view.Backup {
			vs.backupTick = vs.curTick
		} else {
			vs.idleTick = vs.curTick
			vs.idleServer = args.Me
		}
		reply.View = vs.view
		return nil
	}()

	// choose current server as primary
	if vs.view.Primary == "" {
		vs.view.Primary = args.Me
		vs.view.Viewnum++
		vs.needAcked = true
		return nil
	}

	if vs.view.Primary == args.Me {
		if vs.view.Viewnum == args.Viewnum {
			vs.needAcked = false
		} else if args.Viewnum == 0 && vs.view.Backup != "" { // server restart, promote backup server
			vs.view.Primary = vs.view.Backup
			vs.view.Backup = ""
			vs.primaryTick = vs.backupTick
			changed = true
		}
		return nil
	}

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	vs.Lock()
	defer vs.Unlock()
	reply.View = vs.view
	return nil
}

func (vs *ViewServer) isPrimaryCrash() bool {
	return vs.curTick-vs.primaryTick > DeadPings
}

func (vs *ViewServer) isBackupCrash() bool {
	return vs.view.Backup != "" && vs.curTick-vs.backupTick > DeadPings
}

func (vs *ViewServer) hasIdleServer() bool {
	return vs.curTick-vs.idleTick < DeadPings
}

func (vs *ViewServer) tick() {
	vs.Lock()
	defer vs.Unlock()
	vs.curTick++

	// if current view hasn't yet been acked, we shouldn't change view
	if vs.needAcked || vs.view.Primary == "" {
		return
	}
	changed := false
	needPromoteBackup := vs.view.Backup == "" || vs.isBackupCrash()

	// primary server crash
	if vs.isPrimaryCrash() && vs.view.Backup != "" && !vs.isBackupCrash() {
		// has backup server, promote it
		vs.primaryTick = vs.backupTick
		vs.view.Primary = vs.view.Backup
		vs.view.Backup = ""
		changed = true
		needPromoteBackup = true
	}
	// backup server
	if needPromoteBackup {
		if vs.hasIdleServer() {
			vs.view.Backup = vs.idleServer
			vs.backupTick = vs.idleTick
			changed = true

		} else if vs.view.Backup != "" {
			vs.view.Backup = ""
			changed = true

		}
	}
	if changed {
		vs.needAcked = true
		vs.view.Viewnum++
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
