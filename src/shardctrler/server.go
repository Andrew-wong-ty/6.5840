package shardctrler

import (
	"6.5840/raft"
	"fmt"
	"sync/atomic"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type OpType string

const (
	JOIN  OpType = "join"
	LEAVE OpType = "leave"
	MOVE  OpType = "move"
	QUERY OpType = "Query"
)

type ShardCtrler struct {
	dead               int32
	me                 int
	rf                 *raft.Raft
	applyCh            chan raft.ApplyMsg
	mu                 sync.Mutex // protects all the followings
	lastApplied        int
	configs            []Config                  // indexed by config num
	opDoneChans        map[int64]map[int]chan Op // used for notify an Op is done (unique id -> {commitIdx -> chan}
	clientId2SerialNum map[int64]uint64          // keep tracks of the current SerialNum of a client
	joinArgChan        map[int]chan JoinArgs     // commitId -> arg
	leaveArgChan       map[int]chan LeaveArgs    // commitId -> arg
	queryResultChan    map[int]chan Config       // commitId -> cfg
}

// Op represents an operation that is going to be applied on the shard controller
type Op struct {
	ErrMsg        Err
	ClientId      int64
	TransId       int64
	SerialNum     uint64
	OperationType OpType
	// Join(servers) -- add a set of groups (gid -> server-list mapping).
	SerializedServersJoined string
	// Leave(gids) -- delete a set of groups.
	SerializedGidLeaved string
	// Move(shard, gid) -- hand off one shard from current owner to gid.
	ShardMoved int
	GidMovedTo int
	// Query
	QueryNum           int
	SerializedQueryRes string
	QueryLenCfg        int
}

// String provides a string representation of an Op.
func (o *Op) String() string {
	if o.OperationType == JOIN {
		return fmt.Sprintf("Op{ op: %v, ErrMsg: %v,  ClientId: %v,  SerialNum: %v, transId=%v,  ServersJoined: %v, }",
			o.OperationType, o.ErrMsg, o.ClientId, o.SerialNum, o.TransId, deserializeServersJoined(o.SerializedServersJoined))
	} else if o.OperationType == LEAVE {
		return fmt.Sprintf("Op{ op: %v, ErrMsg: %v,  ClientId: %v,  SerialNum: %v, transId=%v, gidsLeave: %v, }",
			o.OperationType, o.ErrMsg, o.ClientId, o.SerialNum, o.TransId, deserializeGidLeaved(o.SerializedGidLeaved))
	} else if o.OperationType == MOVE {
		return fmt.Sprintf("Op{ op: %v, ErrMsg: %v,  ClientId: %v,  SerialNum: %v, transId=%v, ShardMoved: %v, GidMovedTo: %v }",
			o.OperationType, o.ErrMsg, o.ClientId, o.SerialNum, o.TransId, o.ShardMoved, o.GidMovedTo)
	} else if o.OperationType == QUERY {
		return fmt.Sprintf("Op{ op: %v, ErrMsg: %v,  ClientId: %v,  SerialNum: %v, transId=%v, QueryNum: %v, QueryRe: %v, len=%v }",
			o.OperationType, o.ErrMsg, o.ClientId, o.SerialNum, o.TransId, o.QueryNum, deserializeQueryRes(o.SerializedQueryRes), o.QueryLenCfg)
	} else {
		return fmt.Sprintf("Op{ op: %v, ErrMsg: %v,  ClientId: %v,  SerialNum: %v, transId=%v }",
			o.OperationType, o.ErrMsg, o.ClientId, o.SerialNum, o.TransId)
	}

}

func (sc *ShardCtrler) deleteOpDoneChan(uid int64, commandIdx int) {
	sc.mu.Lock()
	delete(sc.opDoneChans[uid], commandIdx)
	if len(sc.opDoneChans[uid]) == 0 {
		delete(sc.opDoneChans, uid)
	}
	sc.mu.Unlock()
}

// getOpDoneChan returns an op-done-notification-chan for a given commandIdx
func (sc *ShardCtrler) getOpDoneChan(uid int64, commandIdx int) chan Op {
	if _, hasClientIdKey := sc.opDoneChans[uid]; !hasClientIdKey {
		sc.opDoneChans[uid] = make(map[int]chan Op)
	}
	if _, hasKey := sc.opDoneChans[uid][commandIdx]; !hasKey {
		sc.opDoneChans[uid][commandIdx] = make(chan Op, 8) // Note: must be unbuffered to avoid deadlock
	}
	return sc.opDoneChans[uid][commandIdx]
}

// Kill is called by the tester when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// Raft is needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// StartServer starts the shard controller.
//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.dead = 0
	sc.me = me
	// initialize the first configuration
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.rf.SetHeartbeatTimeout(40)

	// Your code here.
	sc.clientId2SerialNum = make(map[int64]uint64)
	sc.opDoneChans = make(map[int64]map[int]chan Op)
	sc.lastApplied = 0
	go sc.applier()
	DebugLog(dQuery, nil, "shardctrler starts")
	return sc
}
