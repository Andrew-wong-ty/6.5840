package shardctrler

// The Query RPC's argument is a configuration number.
// The shardctrler replies with the configuration that has that number.
// If the number is -1 or bigger than the biggest known configuration number,
// the shardctrler should reply with the latest configuration.
// The result of Query(-1) should reflect every Join, Leave, or Move RPC that the shardctrler finished handling before it received the Query(-1) RPC.
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Check it this machine is the leader; if not return
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = "this machine is not leader"
		reply.WrongLeader = true
		return
	}
	// check if this request is outdated or duplicated
	sc.mu.Lock()
	if args.SerialNum <= sc.clientId2SerialNum[args.ClientId] {
		reply.Err = "this is an outdated or duplicated request"
		DebugLog(dQuery, sc, "this is an outdated or duplicated request")
		reply.WrongLeader = false
		sc.mu.Unlock()
		return
	}

	// create a unique identifier for this operation
	op := Op{
		ClientId:      args.ClientId,
		SerialNum:     args.SerialNum,
		OperationType: QUERY,
		QueryNum:      args.Num,
	}
	// Start an agreement on this op.
	commandIdx, _, _ := sc.rf.Start(op)
	opDoneChan := sc.getOpDoneChan(commandIdx)
	sc.mu.Unlock()
	DebugLog(dQuery, sc, "start new agreement, cmdIdx=%v", commandIdx)
	// Wait until this agreement is applied (timeout is introduced).
	select {
	case doneOp := <-opDoneChan:
		if doneOp.SerialNum == args.SerialNum && doneOp.ClientId == args.ClientId {
			reply.Err = doneOp.ErrMsg
			reply.WrongLeader = false
			reply.Config = deserializeQueryRes(doneOp.SerializedQueryRes)
			DebugLog(dQuery, sc, "query(%v) done, len=%v, rescfg=%v", args.Num, doneOp.QueryLenCfg, reply.Config)
		} else {
			panic("bug1")
		}
	}
	// GC
	go func() {
		sc.mu.Lock()
		delete(sc.opDoneChans, commandIdx)
		sc.mu.Unlock()
	}()
}
