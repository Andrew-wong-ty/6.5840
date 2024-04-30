package shardctrler

import (
	"fmt"
	"time"
)

// The Query RPC's argument is a configuration number.
// The shardctrler replies with the configuration that has that number.
// If the number is -1 or bigger than the biggest known configuration number,
// the shardctrler should reply with the latest configuration.
// The result of Query(-1) should reflect every Join, Leave, or Move RPC that the shardctrler finished handling before it received the Query(-1) RPC.
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	DebugLog(dQuery, sc, "Query called, trans=%v", args.TransId)
	if sc.killed() {
		reply.WrongLeader = true
		return
	}
	// Check it this machine is the leader; if not return
	if _, isLeader := sc.rf.GetState(); !isLeader {
		DebugLog(dQuery, sc, "this machine is not leader, trans=%v", args.TransId)
		reply.WrongLeader = true
		return
	}
	// check if this request is outdated or duplicated
	DebugLog(dQuery, sc, "try lock, trans=%v", args.TransId)
	sc.mu.Lock()
	DebugLog(dQuery, sc, "locked, trans=%v", args.TransId)
	if args.SerialNum <= sc.clientId2SerialNum[args.ClientId] {
		DebugLog(dQuery, sc, "this is an outdated or duplicated request")
		reply.WrongLeader = false
		sc.mu.Unlock()
		return
	}

	// create a unique identifier for this operation
	op := Op{
		TransId:       args.TransId,
		ClientId:      args.ClientId,
		SerialNum:     args.SerialNum,
		OperationType: QUERY,
		QueryNum:      args.Num,
	}
	// Start an agreement on this op.
	DebugLog(dQuery, sc, "starting agreement, trans=%v", args.TransId)
	commandIdx, _, _ := sc.rf.Start(op)
	if commandIdx == -1 {
		DebugLog(dQuery, sc, "commandIdx == -1, trans=%v", args.TransId)
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	opDoneChan := sc.getOpDoneChan(args.TransId, commandIdx)
	sc.mu.Unlock()
	DebugLog(dQuery, sc, "query start new agreement, cmdIdx=%v, trans=%v, cliId=%v, sNum=%v",
		commandIdx, op.TransId, op.ClientId, op.SerialNum)
	// Wait until this agreement is applied (timeout is introduced).
	timer := time.NewTimer(500 * time.Millisecond)
	select {
	case doneOp := <-opDoneChan:
		if doneOp.SerialNum == args.SerialNum && doneOp.ClientId == args.ClientId && doneOp.TransId == args.TransId {
			reply.Err = doneOp.ErrMsg
			reply.WrongLeader = false
			reply.Config = deserializeQueryRes(doneOp.SerializedQueryRes)
			DebugLog(dQuery, sc, "query(%v) done, len=%v, rescfg=%v", args.Num, doneOp.QueryLenCfg, reply.Config)
		} else {
			panic(fmt.Sprintf("warning: bug, doneOp=(cliID=%v, sNum=%v, trans=%v), args=(cliID=%v, sNum=%v, trans=%v)",
				doneOp.ClientId, doneOp.SerialNum, doneOp.TransId, args.ClientId, args.SerialNum, op.TransId))
			// if the code comes there, the case is: it tries to start the agreement in Raft, however the commit is failed
			// because role transfer, So the commandIdx (commit Idx) here will not be applied anymore. However,
			// some other Query/Join/Leave/Move may use the same commandIdx, and it is finally applied. In this case,
			// two threads are waiting to receive from the chan. If the thread here is not the desired receiver, it should
			// do nothing.
			//panic("bug persist")
			//reply.WrongLeader = true
		}
	case <-timer.C:
		reply.WrongLeader = true
	}
	// GC
	sc.deleteOpDoneChan(args.TransId, commandIdx)
}
