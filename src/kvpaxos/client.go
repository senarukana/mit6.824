package kvpaxos

import "net/rpc"
import crand "crypto/rand"
import "math/big"
import "math/rand"
import "time"

import "fmt"

type Clerk struct {
	servers    []string
	identifier int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.identifier = nrand()
	return ck
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

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{
		Key: key,
	}
	var reply GetReply

	for {
		server := ck.servers[rand.Int()%len(ck.servers)]
		ok := call(server, "KVPaxos.Get", args, &reply)
		if ok && reply.Err == "" {
			break
		}
		reply.Err = ""
		time.Sleep(time.Millisecond * 100)
	}
	return reply.Value
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	for {
		var reply PutAppendReply
		server := ck.servers[rand.Int()%len(ck.servers)]
		ok := call(server, "KVPaxos.PutAppend", args, &reply)
		if ok && reply.Err == "" {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
