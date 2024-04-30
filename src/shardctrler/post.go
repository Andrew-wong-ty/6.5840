package shardctrler

import "time"

// Post is an RPC handler to handle Join/Leave/Move
func (sc *ShardCtrler) Post(args *PostArgs, reply *PostReply) {
	if sc.killed() {
		reply.WrongLeader = true
		return
	}
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	// check if this request is outdated or duplicated
	sc.mu.Lock()
	if args.SerialNum <= sc.clientId2SerialNum[args.ClientId] {
		reply.WrongLeader = false
		sc.mu.Unlock()
		return
	}

	// create a unique identifier for this operation
	op := getPostOp(args)
	// Start an agreement on this op.
	commandIdx, _, _ := sc.rf.Start(op)

	if commandIdx == -1 {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	opDoneChan := sc.getOpDoneChan(args.TransId, commandIdx)
	sc.mu.Unlock()
	DebugLog(dJoin, sc, "%v start new agreement, cmdIdx=%v, trans=%v, cliId=%v, sNum=%v",
		args.OperationType, commandIdx, op.TransId, op.ClientId, op.SerialNum)
	// Wait until this agreement is applied (timeout is introduced).
	timer := time.NewTimer(500 * time.Millisecond)
	select {
	case msg := <-opDoneChan:
		if msg.SerialNum == args.SerialNum && msg.ClientId == args.ClientId && msg.TransId == args.TransId {
			reply.Err = msg.ErrMsg
			reply.WrongLeader = false
			DebugLog(dJoin, sc, "%v done, SerialNum=%v", msg.OperationType, msg.SerialNum)
		} else {
			panic("bug1")
		}
	case <-timer.C:
		reply.WrongLeader = true
	}
	// GC
	sc.deleteOpDoneChan(args.TransId, commandIdx)
}

func getPostOp(args *PostArgs) Op {
	var op Op
	op.TransId = args.TransId
	op.ClientId = args.ClientId
	op.SerialNum = args.SerialNum
	op.OperationType = args.OperationType
	if args.OperationType == JOIN {
		op.SerializedServersJoined = serialize(args.Servers)
		return op
	}
	if args.OperationType == LEAVE {
		op.SerializedGidLeaved = serialize(args.GIDs)
		return op
	}
	if args.OperationType == MOVE {
		op.ShardMoved = args.Shard
		op.GidMovedTo = args.GID
		return op
	}
	panic("unexpected type")
}
