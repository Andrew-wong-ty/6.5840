package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrRepeatedRequest = "ErrRepeatedRequest"
	ErrConfigNotReady  = "ErrConfigNotReady"
)

const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
)

type Err string

// PutAppendArgs Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientID  int64
	SerialNum uint64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientID  int64
	SerialNum uint64
}

type GetReply struct {
	Err   Err
	Value string
}
