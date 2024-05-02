package shardkv

import (
	"time"
)

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// check killed
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	DebugLog(dPut, kv, "%v, key=%v (shard=%v) v=%v try to started", args.Op, args.Key, key2shard(args.Key), args.Value)
	// check leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	// check if cfg is ready
	if kv.currCfg.Num == 0 {
		kv.mu.Unlock()
		reply.Err = ErrConfigNotReady
		return
	}
	// check if the server's replica group is responsible for this key.
	if kv.currCfg.Shards[key2shard(args.Key)] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
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

	// wait until response
	timer := time.NewTimer(requestTimeOut)
	select {
	case msg := <-opDoneChan:
		if msg.ClientId == args.ClientID && msg.SerialNum == args.SerialNum && msg.OpType == args.Op {
			reply.Err = msg.Error
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-timer.C:
		DebugLog(dPut, kv, "%v, key=%v (shard=%v) timeout", op.OpType, op.Key, key2shard(op.Key))
	}
}
