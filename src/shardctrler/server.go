package shardctrler

import (
	"6.5840/raft"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type OpType string

const (
	JOIN  OpType = "join"
	LEAVE OpType = "leave"
	Move  OpType = "move"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	seqNumber int
	configs   []Config // indexed by config num
}

// Op represents an operation that is going to be applied on the shard controller
type Op struct {
	OperationType OpType
	// Join(servers) -- add a set of groups (gid -> server-list mapping).
	ServersJoined map[int][]string
	// Leave(gids) -- delete a set of groups.
	GidLeaved []int
	// Move(shard, gid) -- hand off one shard from current owner to gid.
	ShardMoved int
	GidMovedTo int
}

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
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// Check it this machine is the leader; if not return
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = "this machine is not the leader"
		reply.WrongLeader = true
	}
	// Create a new Op
	op := Op{
		OperationType: JOIN,
		ServersJoined: args.Servers,
	}
	// Start an agreement on this op.
	sc.rf.Start(op)
	// Wait until this agreement is applied (timeout may be introduced).
	select {
	case msg := <-sc.applyCh:
		if msg.CommandValid {
			reply.Err = ""
			reply.WrongLeader = false
		} else {
			panic("shard controller does not support snapshot")
		}
	case <-time.After(time.Second):
		reply.Err = "raft agreement timeout"
		reply.WrongLeader = false
	}
	// After the agreement is applied, add this new agreement to the configs slice
	if sc.seqNumber == -1 { // the first config
		sc.seqNumber++
		sc.configs = append(sc.configs, Config{
			Num:    sc.seqNumber,
			Shards: [10]int{},
			Groups: nil,
		})
	}
	//var prevShards [NShards]int
	//var prevGroups map[int][]string
	//if sc.seqNumber != -1 {
	//	prevShards = sc.configs[sc.seqNumber].Shards
	//	prevGroups = sc.configs[sc.seqNumber].Groups
	//}
	//sc.seqNumber++
	//sc.configs = append(sc.configs, Config{
	//	Num:    sc.seqNumber,
	//	Shards: [10]int{},
	//	Groups: nil,
	//})

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

// The Query RPC's argument is a configuration number.
// The shardctrler replies with the configuration that has that number.
// If the number is -1 or bigger than the biggest known configuration number,
// the shardctrler should reply with the latest configuration.
// The result of Query(-1) should reflect every Join, Leave, or Move RPC that the shardctrler finished handling before it received the Query(-1) RPC.
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// check leader
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Config = Config{}
		reply.Err = ""
		reply.WrongLeader = true
		return
	}
	var latestConfig Config
	for _, cfg := range sc.configs {
		if cfg.Num == args.Num {
			reply.Config = cfg
			reply.Err = ""
			reply.WrongLeader = false
			return
		}
		latestConfig = cfg
	}
	if args.Num == -1 { // number is -1 or the number is not found
		reply.Config = latestConfig
		reply.Err = ""
		reply.WrongLeader = false
		return
	}
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

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.seqNumber = -1
	return sc
}
