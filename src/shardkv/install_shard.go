package shardkv

import (
	"6.5840/shardctrler"
	"fmt"
	"time"
)

type InstallShardArgs struct {
	ToGid            int    // receiver's GID
	Data             string // serialized (gob+base64) shard data of type [NShard]map[string][string]
	ShardIDs         string // the shard IDs to be installed; serialized (gob + base64)
	ClientID         int64
	Client2SerialNum string
	CfgNum           int // = currCfg.Num
}

type InstallShardReply struct {
	ErrMsg                 Err
	InstalledSuccessShards string // the shard IDs successfully installed
}

// sendInstallShardData sends RPC call to the leader of a GID based on a Config until success
/****** RPC sender ******/
func (kv *ShardKV) sendInstallShardData(cfg shardctrler.Config, args *InstallShardArgs) (bool, string) {
	if serverNames, exist := cfg.Groups[args.ToGid]; exist {
		nTry := 0
		for sid := 0; ; sid++ {
			sid = sid % len(serverNames)
			if nTry != 0 && nTry%len(serverNames) == 0 {
				time.Sleep(retryInterval)
			}

			srv := kv.make_end(serverNames[sid])
			reply := InstallShardReply{}
			ok := srv.Call("ShardKV.InstallShardData", args, &reply)
			if ok && (reply.ErrMsg == OK) {
				return true, reply.InstalledSuccessShards
			}

			nTry++
		}
	} else {
		panic(fmt.Sprintf("unexpected, gid:%v does not exist in this config:%v", args.ToGid, cfg))
	}
	return false, encodeSlice([]int{})
}

// InstallShardData is the handler for the RPC call from another server who wants to migrate
// its shard data to this server.
// The function first start an agreement in Raft, then wait for the applier to do the multiple-put on shard data
/****** RPC handler ******/
func (kv *ShardKV) InstallShardData(args *InstallShardArgs, reply *InstallShardReply) {
	if args.ToGid != kv.gid {
		panic("wtf?")
	}
	if kv.killed() {
		reply.ErrMsg = ErrWrongLeader
		return
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.ErrMsg = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	op := Op{
		OpType:           INSTALLSHARD,
		ShardData:        args.Data,
		ShardIDs:         args.ShardIDs,
		ShardDataVersion: args.CfgNum,
		Client2SerialNum: args.Client2SerialNum,
		ClientId:         args.ClientID,
		SerialNum:        uint64(args.CfgNum),
	}

	commandIdx, _, _ := kv.rf.Start(op)
	opDoneChan := kv.getOpDoneChan(commandIdx)
	kv.mu.Unlock()
	DebugLog(dMigrate, kv, "migrate shards agreement started, shardIds=%v, v=%v", decodeSlice(args.ShardIDs), args.CfgNum)
	// wait until response
	timer := time.NewTimer(requestTimeOut)
	select {
	case msg := <-opDoneChan:
		reply.ErrMsg = msg.Error
		reply.InstalledSuccessShards = msg.InstalledSuccessShards
	case <-timer.C:
		DebugLog(dGet, kv, "install shard=%v timeout", decodeSlice(args.ShardIDs))
	}
}
