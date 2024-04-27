package shardctrler

import (
	"6.5840/raft"
	"fmt"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

// TODO: 1. remove the Join, Leave, Move, Query's duplicated code
// TODO: 2. logically divide the functions (e.g. the ones in the applier) into several .go files

type OpType string

const (
	JOIN  OpType = "join"
	LEAVE OpType = "leave"
	MOVE  OpType = "move"
	QUERY OpType = "Query"
)

type ShardCtrler struct {
	me                 int
	rf                 *raft.Raft
	applyCh            chan raft.ApplyMsg
	mu                 sync.Mutex // protects all the followings
	lastApplied        int
	configs            []Config               // indexed by config num
	opDoneChans        map[int]chan Op        // used for notify an Op is done
	clientId2SerialNum map[int64]uint64       // keep tracks of the current SerialNum of a client
	joinArgChan        map[int]chan JoinArgs  // commitId -> arg
	leaveArgChan       map[int]chan LeaveArgs // commitId -> arg
	queryResultChan    map[int]chan Config    // commitId -> cfg
}

// Op represents an operation that is going to be applied on the shard controller
type Op struct {
	ErrMsg        Err
	ClientId      int64
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

func (c *Config) String() string {
	return fmt.Sprintf("Config{Num: %d, Shards: %v, Groups: %v}", c.Num, c.Shards, c.Groups)
}

// String provides a string representation of an Op.
func (o *Op) String() string {
	return fmt.Sprintf("Op{\n  ErrMsg: %v,\n  ClientId: %d,\n  SerialNum: %d,\n  OperationType: %v,\n  ServersJoined: %v,\n  GidLeaved: %v,\n  ShardMoved: %d,\n  GidMovedTo: %d,\n  QueryNum: %d,\n  QueryRes: %v,\n  QueryLenCfg: %d\n}",
		o.ErrMsg, o.ClientId, o.SerialNum, o.OperationType, o.SerializedServersJoined, o.SerializedGidLeaved, o.ShardMoved, o.GidMovedTo, o.QueryNum, o.SerializedQueryRes, o.QueryLenCfg)
}

// getOpDoneChan returns an op-done-notification-chan for a given commandIdx
func (sc *ShardCtrler) getOpDoneChan(commandIdx int) chan Op {
	if _, hasKey := sc.opDoneChans[commandIdx]; !hasKey {
		sc.opDoneChans[commandIdx] = make(chan Op, 1) // Note: must be unbuffered to avoid deadlock
	}
	return sc.opDoneChans[commandIdx]
}

// Kill is called by the tester when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
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
	sc.me = me
	// initialize the first configuration
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.rf.SetHeartbeatTimeout(32)

	// Your code here.
	sc.clientId2SerialNum = make(map[int64]uint64)
	sc.opDoneChans = make(map[int]chan Op)
	sc.lastApplied = 0
	go sc.applier()
	return sc
}
