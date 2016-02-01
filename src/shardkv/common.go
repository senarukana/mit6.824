package shardkv

import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey      Err = "ErrNoKey"
	ErrWrongGroup Err = "ErrWrongGroup"
)

type Err string
type Operation string

const (
	PUT_OPERATION        Operation = "PUT"
	APPEND_OPERATION               = "APPEND"
	GET_OPERATION                  = "GET"
	SHARD_SYNC_OPERATION           = "SHARD_SYNC"
	RECONFIG_OPERATION             = "RECONFIG"
)

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64
}

type GetReply struct {
	Err   Err
	Value string
}

type ReconfigArgs struct {
	Id     int
	Config *shardmaster.Config
}

type ShardSyncArgs struct {
	ShardId int
	Id      int64
}

type ShardSyncReply struct {
	Err   Err
	Shard *Shard
}
