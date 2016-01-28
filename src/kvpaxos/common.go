package kvpaxos

import "fmt"

const (
	OK           = "OK"
	ErrNoKey Err = "ErrNoKey"
)

type Err string
type Operation string

const (
	PUT_OPERATION    Operation = "PUT"
	APPEND_OPERATION           = "APPEND"
	GET_OPERATION              = "GET"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Token int64
}

func (a *PutAppendArgs) String() string {
	return fmt.Sprintf("key %s, val %s, token %d", a.Key, a.Value, a.Token)
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Token int64
}

func (a *GetArgs) String() string {
	return fmt.Sprintf("key %s", a.Key)
}

type GetReply struct {
	Err   Err
	Value string
}
