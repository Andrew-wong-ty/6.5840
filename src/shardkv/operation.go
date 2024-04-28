package shardkv

import "fmt"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key          string
	Value        string
	ResultForGet string // the result for get
	OpType       string // GET or PUT or APPEND
	SerialNum    uint64 // client request's serial number
	ClientId     int64
	Error        Err
}

func (op *Op) String() string {
	res := fmt.Sprintf("key=%v, value=%v, opType=%v, seqNum=%v, clientId=%v",
		op.Key, op.Value, op.OpType, op.SerialNum, op.ClientId)
	return res
}
