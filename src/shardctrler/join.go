package shardctrler

// Join is an RPC handler. The Join RPC is used by an administrator to add new replica groups.
// Its argument is a set of mappings from unique, non-zero replica group identifiers (GIDs) to lists of server names.
// The shardctrler should react by creating a new configuration that includes the new replica groups.
// The new configuration should divide the shards as evenly as possible among the full set of groups,
//
//	and should move as few shards as possible to achieve that goal.
//
// The shardctrler should allow re-use of a GID if it's not part of the current configuration
//
//	(i.e. a GID should be allowed to Join, then Leave, then Join again).
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
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
		TransId:                 args.TransId,
		ClientId:                args.ClientId,
		SerialNum:               args.SerialNum,
		OperationType:           JOIN,
		SerializedServersJoined: serialize(args.Servers),
	}
	// Start an agreement on this op.
	commandIdx, _, _ := sc.rf.Start(op)
	if commandIdx == -1 {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	opDoneChan := sc.getOpDoneChan(args.TransId, commandIdx)
	sc.mu.Unlock()
	DebugLog(dJoin, sc, "join start new agreement, cmdIdx=%v, trans=%v, cliId=%v, sNum=%v",
		commandIdx, op.TransId, op.ClientId, op.SerialNum)
	// Wait until this agreement is applied (timeout is introduced).
	select {
	case doneOp := <-opDoneChan:
		if doneOp.SerialNum == args.SerialNum && doneOp.ClientId == args.ClientId && doneOp.TransId == args.TransId {
			reply.Err = doneOp.ErrMsg
			reply.WrongLeader = false
			DebugLog(dJoin, sc, "join done, SerialNum=%v", doneOp.SerialNum)
		} else {
			panic("bug1")
		}
	}
	// GC
	go sc.deleteOpDoneChan(args.TransId, commandIdx)
	//go func() {
	//	sc.mu.Lock()
	//	delete(sc.opDoneChans[args.TransId], commandIdx)
	//	if len(sc.opDoneChans[args.TransId]) == 0 {
	//		delete(sc.opDoneChans, args.TransId)
	//	}
	//	sc.mu.Unlock()
	//}()
}
