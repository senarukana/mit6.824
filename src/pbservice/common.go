package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string
type Operation int

const (
	PUT_OPERATION Operation = iota
	APPEND_OPERATION
	REPLICATE_OPERATION
	GET_OPERATION
)

const (
	IDLE int32 = iota
	PRIMARY
	BACKUP
)

type OperationArgs struct {
	op         Operation
	args       interface{}
	resultChan chan interface{}
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Operation Operation
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Token            int64
	ClientIdentifier int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

type ReplicateArgs struct {
	Data map[string]*Value
}

// Your RPC definitions here.
