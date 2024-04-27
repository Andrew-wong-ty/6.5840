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
		ClientId:                args.ClientId,
		SerialNum:               args.SerialNum,
		OperationType:           JOIN,
		SerializedServersJoined: serialize(args.Servers),
	}
	// Start an agreement on this op.
	commandIdx, _, _ := sc.rf.Start(op)
	opDoneChan := sc.getOpDoneChan(commandIdx)
	sc.mu.Unlock()
	DebugLog(dJoin, sc, "start new agreement, cmdIdx=%v", commandIdx)
	// Wait until this agreement is applied (timeout is introduced).
	select {
	case doneOp := <-opDoneChan:
		if doneOp.SerialNum == args.SerialNum && doneOp.ClientId == args.ClientId {
			reply.Err = doneOp.ErrMsg
			reply.WrongLeader = false
			DebugLog(dJoin, sc, "join done, SerialNum=%v", doneOp.SerialNum)
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
