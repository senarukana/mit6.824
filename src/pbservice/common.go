package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string
type Operation int
type State int

const (
	PUT_OPERATION Operation = iota
	APPEND_OPERATION
	GET_OPERATION
)

const (
	IDLE State = iota
	PRIMARY
	BACKUP
)

type OperationArgs struct {
	op         Operation
	args       interface{}
	resultChan chan interface{}
}

type RepliOperationArgs struct {
	*PutAppendArgs
	doneChan chan bool
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Operation Operation
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Token int64
}

type ReplicationArgs struct {
	Me string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Token int64
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
