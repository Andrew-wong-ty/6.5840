package shardctrler

// The Move RPC's arguments are a shard number and a GID.
// The shardctrler should create a new configuration in which the shard is assigned to the group.
// The purpose of Move is to allow us to test your software.
// A Join or Leave following a Move will likely un-do the Move, since Join and Leave re-balance.
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
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
		ClientId:      args.ClientId,
		SerialNum:     args.SerialNum,
		OperationType: MOVE,
		ShardMoved:    args.Shard,
		GidMovedTo:    args.GID,
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
			DebugLog(dMove, sc, "move done, SerialNum=%v", doneOp.SerialNum)
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
