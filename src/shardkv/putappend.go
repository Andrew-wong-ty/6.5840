package shardkv

import "fmt"

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// TODO: should the config-ready-check be here of in the applier?
	if pass, err := kv.doCommonChecks(args.Key); !pass {
		reply.Err = err
		return
	}
	kv.mu.Lock()
	// check serial number
	if args.SerialNum <= kv.clientId2SerialNum[args.ClientID] {
		reply.Err = ErrRepeatedRequest
		kv.mu.Unlock()
		return
	}
	// start agreement
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		OpType:    args.Op,
		SerialNum: args.SerialNum,
		ClientId:  args.ClientID,
	}
	commandIdx, _, _ := kv.rf.Start(op)
	opDoneChan := kv.getOpDoneChan(commandIdx)
	kv.mu.Unlock()
	DebugLog(dPut, kv, "put/append started")
	// wait until response
	select {
	case msg := <-opDoneChan:
		if msg.ClientId == args.ClientID && msg.SerialNum == args.SerialNum && msg.OpType == args.Op {
			reply.Err = OK
		} else {
			panic(fmt.Sprintf("unmatch clientId/SerialNum/opType, msg=%v, args=%v", msg, args))
		}
	}
	go kv.deleteKeyFromOpDoneChans(commandIdx)
}
