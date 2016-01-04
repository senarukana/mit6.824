package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"
import "math/rand"

type ViewServer struct {
	sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	view         View
	nextView     *View
	waitedBackup string
	needAcked    bool

	serverStat map[string]bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.Lock()
	defer vs.Unlock()
	vs.serverStat[args.Me] = true
	defer func() error {
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
		} else if args.Viewnum == 0 { // server restart
			if vs.view.Backup != "" { // promote backup server
				vs.view.Primary = vs.view.Backup
				vs.view.Backup = ""
				vs.view.Viewnum++
				vs.needAcked = true
			}
			// no backup, vs is hanged, need to wait for primary server recover ...
		}
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

func (vs *ViewServer) tick() {
	vs.Lock()
	defer vs.Unlock()

	// if current view hasn't yet been acked, we shouldn't change view
	if vs.needAcked || vs.view.Primary == "" {
		return
	}
	changed := false
	var idleServers []string
	for server, alive := range vs.serverStat {
		if alive && server != vs.view.Primary && server != vs.view.Backup {
			idleServers = append(idleServers, server)
		}
	}

	isPrimaryCrash := !vs.serverStat[vs.view.Primary]
	needChoseBackup := false
	if vs.view.Backup == "" || !vs.serverStat[vs.view.Backup] {
		needChoseBackup = true
	}
	// primary server crash
	if isPrimaryCrash {
		// has backup server, promote it
		if !needChoseBackup {
			vs.view.Primary = vs.view.Backup
			vs.view.Backup = ""
			changed = true
			needChoseBackup = true
		} else {
			return // no backup server, vs is hanged, need to wait for primary server recover ...
		}
	}
	// backup server
	if needChoseBackup {
		// randomly choose a backup from idle server
		if len(idleServers) != 0 {
			backupServer := idleServers[rand.Int()%len(idleServers)]
			vs.view.Backup = backupServer
			changed = true
		} else if vs.view.Backup != "" {
			vs.view.Backup = ""
			changed = true
		} else {
			// backup server is empty but there is no idle server
		}
	}
	for server := range vs.serverStat {
		vs.serverStat[server] = false
	}
	if changed {
		vs.needAcked = true
		vs.view.Viewnum++
	}
	time.Sleep(time.Millisecond * 100)
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
	vs.serverStat = make(map[string]bool)
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
