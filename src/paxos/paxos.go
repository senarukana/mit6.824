package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "time"
import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         uint32 // index into peers[]

	// Your data here.
	lock  sync.Mutex
	items map[int]*logItem
	// client set done seq
	doneSeq int64
	// server max seq
	maxSeq int64
	// all peers min done seq
	peerMinSeq int64
	// peers sequence lock
	peersLock   sync.Mutex
	peersMaxSeq []int64
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() %s failed: %v\n", name, err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	go px.propose(seq, v)
}

func (px *Paxos) decided(seq int) bool {
	px.lock.Lock()
	defer px.lock.Unlock()
	item, existed := px.items[seq]
	if existed {
		return item.fate == Decided
	} else {
		return false
	}
}

func (px *Paxos) createRequestNum(seq int, round uint32) *RequestNumber {
	return &RequestNumber{
		Round:    round,
		ServerId: px.me,
	}
}

func (px *Paxos) getRound(seq int) uint32 {
	px.lock.Lock()
	defer px.lock.Unlock()

	var round uint32
	item, existed := px.items[seq]
	if existed {
		round = item.prepareNum.Round + 1
	} else {
		round = 1
	}
	return round
}

func (px *Paxos) sleep() {
	time.Sleep(time.Millisecond * (50 + time.Duration(rand.Int()%100)))
}

func (px *Paxos) maxDoneSeq() int64 {
	doneSeq := atomic.LoadInt64(&px.doneSeq)
	maxSeq := atomic.LoadInt64(&px.maxSeq)
	if doneSeq < maxSeq {
		return doneSeq
	} else {
		return maxSeq
	}
}

func (px *Paxos) updateMaxSeq(seq int) {
	if seq != int(atomic.LoadInt64(&px.maxSeq))+1 {
		return
	}
	px.lock.Lock()
	defer px.lock.Unlock()
	var i int
	for i = seq; ; i++ {
		item, existed := px.items[i]
		if !existed || item.fate != Decided {
			break
		}
	}
	atomic.StoreInt64(&px.maxSeq, int64(i-1))
}

func (px *Paxos) broadcastPrepare(seq int, num *RequestNumber) (v interface{}, success bool) {
	majorityNum := len(px.peers)/2 + 1
	prepareArgs := &PrepareArgs{
		Seq: seq,
		Num: num,
	}
	var acceptedPrepareNum int
	var maxAcceptedNum *RequestNumber
	prepareReplyChan := make(chan *PrepareReply)
	prepareFunc := func(i int) {
		var reply PrepareReply
		if i == int(px.me) {
			px.Prepare(prepareArgs, &reply)
			prepareReplyChan <- &reply
		}
		if ok := call(px.peers[i], "Paxos.Prepare", prepareArgs, &reply); ok {
			prepareReplyChan <- &reply
		} else {
			prepareReplyChan <- nil
		}

	}
	// log.Printf("prepare phase begin\n")
	for i := range px.peers {
		go prepareFunc(i)

	}
	for _ = range px.peers {
		reply := <-prepareReplyChan
		if reply != nil {
			acceptedPrepareNum++
			if reply.AcceptedValue != nil && (maxAcceptedNum == nil || maxAcceptedNum.Less(reply.AcceptedNumber)) {
				v = reply.AcceptedValue
				maxAcceptedNum = reply.AcceptedNumber
			}
		}
	}
	// log.Printf("prepare phase finish num: %d\n", acceptedPrepareNum)
	success = acceptedPrepareNum >= majorityNum
	return
}

func (px *Paxos) broadcastAccept(seq int, num *RequestNumber, v interface{}) (prepareNum *RequestNumber, success bool) {
	success = true
	majorityNum := len(px.peers)/2 + 1
	prepareNum = num
	var acceptedAcceptNum int
	args := &AcceptArgs{
		Seq:   seq,
		Num:   num,
		Value: v,
	}
	replyChan := make(chan *AcceptReply)

	acceptFunc := func(i int) {
		var reply AcceptReply
		if i == int(px.me) {
			px.Accept(args, &reply)
			replyChan <- &reply
		}
		if ok := call(px.peers[i], "Paxos.Accept", args, &reply); ok {
			replyChan <- &reply
		} else {
			replyChan <- nil
		}

	}
	for i := range px.peers {
		go acceptFunc(i)
	}

	for _ = range px.peers {
		reply := <-replyChan
		if reply != nil {
			acceptedAcceptNum++
			if prepareNum.Less(reply.PrepareNumber) {
				prepareNum = reply.PrepareNumber
				success = false
			}
		}
	}

	// log.Printf("prepare phase finish num: %d, success: %v\n", acceptedAcceptNum, success)
	success = success && (acceptedAcceptNum >= majorityNum)
	return
}

func (px *Paxos) broadcastDecided(seq int, num *RequestNumber, v interface{}) {
	decidedArgs := &DecidedArgs{
		Seq:    seq,
		Num:    num,
		Value:  v,
		Source: int(px.me),
		MaxSeq: px.maxDoneSeq(),
	}
	var mustDecidedFunc func(i int)
	mustDecidedFunc = func(i int) {
		var reply DecidedReply
		if uint32(i) == px.me {
			px.Decided(decidedArgs, &reply)
			return
		}
		if ok := call(px.peers[i], "Paxos.Decided", decidedArgs, &reply); ok {
			return
		} else {
			px.sleep()
			go mustDecidedFunc(i)
		}
	}
	for i := range px.peers {
		go mustDecidedFunc(i)
	}
}

func (px *Paxos) removeDoneItem(seq int64) {
	for i := px.peerMinSeq; i <= seq; i++ {
		delete(px.items, int(i))
	}
}

func (px *Paxos) updateMinPeerSeq(source int, maxSeq int64) {
	px.peersLock.Lock()
	defer px.peersLock.Unlock()
	if px.peersMaxSeq[source] >= maxSeq {
		return
	}

	px.peersMaxSeq[source] = maxSeq
	peerMinSeq := maxSeq
	for _, peerMaxSeq := range px.peersMaxSeq {
		if peerMaxSeq < peerMinSeq {
			peerMinSeq = peerMaxSeq
		}
	}
	if atomic.LoadInt64(&px.peerMinSeq) < peerMinSeq {
		px.removeDoneItem(peerMinSeq)
		px.peerMinSeq = peerMinSeq
	}
}

func (px *Paxos) propose(seq int, v interface{}) {
	round := px.getRound(seq)
	for !px.isdead() && !px.decided(seq) {
		num := px.createRequestNum(seq, round)
		// 2. prepare phase
		newV, success := px.broadcastPrepare(seq, num)
		if !success {
			px.sleep()
			continue
		}
		if newV != nil {
			v = newV
		}
		// 3. accept phase
		maxPrepareNum, success := px.broadcastAccept(seq, num, v)
		if !success {
			round = maxPrepareNum.Round + 1
			px.sleep()
			continue
		}
		if num.Less(maxPrepareNum) {
			panic("xx")
		}
		px.updateMaxSeq(seq)
		// 4. decided phase
		go px.broadcastDecided(seq, num, v)
		return
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	if int64(seq) > atomic.LoadInt64(&px.doneSeq) {
		atomic.StoreInt64(&px.doneSeq, int64(seq))
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	return int(atomic.LoadInt64(&px.maxSeq))
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//

func (px *Paxos) Min() int {
	return int(atomic.LoadInt64(&px.peerMinSeq)) + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	px.lock.Lock()
	defer px.lock.Unlock()

	if int64(seq) <= atomic.LoadInt64(&px.peerMinSeq) {
		return Forgotten, nil
	}
	if item, existed := px.items[seq]; !existed {
		return Pending, nil
	} else {
		return item.fate, item.value
	}

}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.lock.Lock()
	defer px.lock.Unlock()

	item, existed := px.items[args.Seq]
	if !existed {
		px.items[args.Seq] = &logItem{
			prepareNum: args.Num,
		}
	} else {
		if item.acceptedNum != nil {
			reply.AcceptedNumber = item.acceptedNum
			reply.AcceptedValue = item.value
		} else {
			if item.prepareNum.Less(args.Num) {
				item.prepareNum = args.Num
			}
		}
	}
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.lock.Lock()
	defer px.lock.Unlock()
	item, existed := px.items[args.Seq]
	if !existed {
		px.items[args.Seq] = &logItem{
			prepareNum:  args.Num,
			acceptedNum: args.Num,
			value:       args.Value,
		}
	} else {
		if args.Num.Less(item.prepareNum) {
			reply.PrepareNumber = item.prepareNum
		} else {
			item.prepareNum = args.Num
			item.acceptedNum = args.Num
			item.value = args.Value
		}
	}
	return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	log.Printf("[%d] decided, num: %s, seq %d, val: %v\n", px.me, args.Num, args.Seq, args.Value)
	px.lock.Lock()
	px.items[args.Seq] = &logItem{
		prepareNum:  args.Num,
		acceptedNum: args.Num,
		value:       args.Value,
		fate:        Decided,
	}
	px.lock.Unlock()

	px.updateMaxSeq(args.Seq)
	px.updateMinPeerSeq(args.Source, args.MaxSeq)
	return nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = uint32(me)
	px.doneSeq = -1
	px.maxSeq = -1
	px.peerMinSeq = -1
	px.items = make(map[int]*logItem)
	px.peersMaxSeq = make([]int64, len(px.peers))
	for i, _ := range px.peersMaxSeq {
		px.peersMaxSeq[i] = -1
	}

	// Your initialization code here.

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}
	return px
}
