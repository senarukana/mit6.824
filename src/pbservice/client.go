package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

import "log"
import "time"

import "crypto/rand"
import "math/big"

const (
	TICK_THRESHOLD = 2
)

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	view          *viewservice.View
	backupAddress string
	tick          int
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	return ck
}

func MakeReplicationClerk(backupAddress string) *Clerk {
	return &Clerk{
		backupAddress: backupAddress,
	}
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (ck *Clerk) getView() *viewservice.View {
	ck.tick++
	if ck.view == nil || ck.tick == TICK_THRESHOLD {
		// keep trying to get view
		for {
			if view, ok := ck.vs.Get(); ok {
				ck.view = &view
				break
			}
		}
		ck.tick = 0
	}
	return ck.view
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
// slave will never call it
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{}
	args.Key = key
	var reply GetReply
	var ok bool
	for !ok {
		primaryAddress := ck.getView().Primary
		// send an RPC request, wait for the reply.
		ok = call(primaryAddress, "PBServer.Get", args, &reply)
		if ok {
			break
		}
		// log.Printf("get key: %s from primary %s error: %s", key, primaryAddress, reply.Err)
		time.Sleep(time.Millisecond * 50)
	}

	return reply.Value
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op Operation) {
	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		Token:     nrand(),
		Operation: op,
	}
	var ok bool
	var address string
	doneChan := make(chan bool)
	callFunc := func(args *PutAppendArgs, reply *PutAppendReply) {
		ok := call(address, "PBServer.PutAppend", args, reply)
		doneChan <- ok
	}
	for !ok {
		if ck.backupAddress != "" {
			address = ck.backupAddress
		} else {
			address = ck.getView().Primary
		}
		var reply PutAppendReply

		// send an RPC request, wait for the reply.
		tick := time.After(time.Second)
		go callFunc(args, &reply)
		select {
		case <-tick:
			log.Printf("[%s] PutAppend key [%s] timeout\n", address, key)
			continue
		case ok = <-doneChan:
			if ok && reply.Err != "" {
				ok = false
			}
		}
		if ok {
			return
		}
		log.Printf("put key: %s to server %s error: %s", key, address, reply.Err)
		time.Sleep(time.Millisecond * 100)
	}
}

func (ck *Clerk) Replicate(args *PutAppendArgs, reply *PutAppendReply) bool {
	return call(ck.backupAddress, "PBServer.PutAppend", args, reply)
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT_OPERATION)
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND_OPERATION)
}
