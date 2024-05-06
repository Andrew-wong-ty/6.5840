package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"fmt"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

const requestTimeOut = 1 * time.Second
const cfgPollingTimeout = 100 * time.Millisecond

type Shard struct {
	Version int // the shard is ready only when version==kv.currCfg.Num
	Data    map[string]string
}

type ShardKV struct {
	mu                 sync.Mutex // protects the followings till cfgMutex
	me                 int
	rf                 *raft.Raft
	applyCh            chan raft.ApplyMsg
	make_end           func(string) *labrpc.ClientEnd
	gid                int
	ctrlers            []*labrpc.ClientEnd
	maxraftstate       int // snapshot if log grows this big
	persister          *raft.Persister
	dead               int32
	scc                *shardctrler.Clerk         // shard controller clerk.
	opDoneChans        map[int]chan Op            // used for notify an Op is done
	clientId2SerialNum map[int64]uint64           // to prevent duplicate requests
	inMemoryDB         [shardctrler.NShards]Shard // shardId -> DB
	currCfg            shardctrler.Config         // the latest shardctrler.Config; the server polls shardctrler for it periodically
	prevCfg            shardctrler.Config         // the previous config
	clientId           int64                      // when calling InstallShardData RPC, this machine is a client
	lastApplied        int                        // the index of latest applied log
	cfgPollingTicker   *time.Ticker               // periodically send request to poll latest Config
}

// Kill is called by the tester when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
	kv.cfgPollingTicker.Stop()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartServer
//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int,
	ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(Shard{})
	labgob.Register(Err(""))

	kv := new(ShardKV)
	kv.mu = sync.Mutex{}
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.applyCh = make(chan raft.ApplyMsg)
	// make raft
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.SetCommitNoop(true)
	kv.rf.SetDbgMsg(fmt.Sprintf("%v", kv.gid))
	kv.persister = persister
	kv.dead = 0
	kv.scc = shardctrler.MakeClerk(kv.ctrlers)
	kv.lastApplied = 0
	kv.clientId2SerialNum = make(map[int64]uint64)
	kv.opDoneChans = make(map[int]chan Op)
	kv.currCfg = shardctrler.Config{}
	kv.prevCfg = shardctrler.Config{}
	kv.cfgPollingTicker = time.NewTicker(cfgPollingTimeout)
	kv.clientId = nrand()
	// init the DB
	for i := 0; i < shardctrler.NShards; i++ {
		kv.inMemoryDB[i] = Shard{Version: 0, Data: make(map[string]string)}
	}
	// read snapshot from persister (after the machine restart)
	snapshot := persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		clt2SerialNum, db, currCfg, prevCfg, clientId, lastApplied := kv.decodeSnapshot(snapshot)
		kv.clientId2SerialNum = clt2SerialNum
		kv.inMemoryDB = db
		kv.currCfg = currCfg
		kv.prevCfg = prevCfg
		kv.clientId = clientId
		kv.lastApplied = lastApplied
		DebugLog(dSnap, kv, "snapshot installed when server up, currCfg.Num=%v db=%v", currCfg.Num, db2str(kv.inMemoryDB))
	}
	go kv.ticker()
	go kv.applier()
	DebugLog(dCheck, kv, "server started!")

	return kv
}
