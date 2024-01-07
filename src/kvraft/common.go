package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
	NOOP   = "NOOP"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	CommandId int32
	ClientId  int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	CommandId int32
	ClientId  int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type CommandRequest struct {
	TransId   int64
	Key       string
	Value     string
	OpType    string
	SerialNum int
	ClientId  int64
}

func (cr CommandRequest) toString() string {
	res := fmt.Sprintf("key=%v, value=%v, op=%v, seqNum=%v, clientId=%v", cr.Key, cr.Value, cr.OpType, cr.SerialNum, cr.ClientId)
	return res
}

type CommandResponse struct {
	Err      Err
	Value    string
	LeaderId int
}

type Command struct {
	key   string
	value string
	op    string
}
