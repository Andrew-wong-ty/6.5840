package shardctrler

// The Leave RPC's argument is a list of GIDs of previously joined groups.
// The shardctrler should create a new configuration that does not include those groups,
// and that assigns those groups' shards to the remaining groups.
// The new configuration should divide the shards as evenly as possible among the groups,
// and should move as few shards as possible to achieve that goal.
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
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
		reply.WrongLeader = false
		sc.mu.Unlock()
		return
	}

	// create a unique identifier for this operation
	op := Op{
		ClientId:            args.ClientId,
		SerialNum:           args.SerialNum,
		OperationType:       LEAVE,
		SerializedGidLeaved: serialize(args.GIDs),
	}
	// Start an agreement on this op.
	commandIdx, _, _ := sc.rf.Start(op)
	opDoneChan := sc.getOpDoneChan(commandIdx)
	sc.mu.Unlock()

	// Wait until this agreement is applied (timeout is introduced).
	select {
	case doneOp := <-opDoneChan:
		if doneOp.SerialNum == args.SerialNum && doneOp.ClientId == args.ClientId {
			reply.Err = doneOp.ErrMsg
			reply.WrongLeader = false
			DebugLog(dLeave, sc, "leave done, SerialNum=%v", doneOp.SerialNum)
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
