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
	configs            []Config         // indexed by config num
	opDoneChans        map[int]chan Op  // used for notify an Op is done
	clientId2SerialNum map[int64]uint64 // keep tracks of the current SerialNum of a client
}

// Op represents an operation that is going to be applied on the shard controller
type Op struct {
	ErrMsg        Err
	ClientId      int64
	SerialNum     uint64
	OperationType OpType
	// Join(servers) -- add a set of groups (gid -> server-list mapping).
	ServersJoined map[int][]string
	// Leave(gids) -- delete a set of groups.
	GidLeaved []int
	// Move(shard, gid) -- hand off one shard from current owner to gid.
	ShardMoved int
	GidMovedTo int
	// Query
	QueryNum    int
	QueryRes    Config
	QueryLenCfg int
}

func (c *Config) String() string {
	return fmt.Sprintf("Config{Num: %d, Shards: %v, Groups: %v}", c.Num, c.Shards, c.Groups)
}

// String provides a string representation of an Op.
func (o *Op) String() string {
	return fmt.Sprintf("Op{\n  ErrMsg: %v,\n  ClientId: %d,\n  SerialNum: %d,\n  OperationType: %v,\n  ServersJoined: %v,\n  GidLeaved: %v,\n  ShardMoved: %d,\n  GidMovedTo: %d,\n  QueryNum: %d,\n  QueryRes: %v,\n  QueryLenCfg: %d\n}",
		o.ErrMsg, o.ClientId, o.SerialNum, o.OperationType, o.ServersJoined, o.GidLeaved, o.ShardMoved, o.GidMovedTo, o.QueryNum, o.QueryRes, o.QueryLenCfg)
}

// getOpDoneChan returns an op-done-notification-chan for a given commandIdx
func (sc *ShardCtrler) getOpDoneChan(commandIdx int) chan Op {
	if _, hasKey := sc.opDoneChans[commandIdx]; !hasKey {
		sc.opDoneChans[commandIdx] = make(chan Op, 1) // Note: must be unbuffered to avoid deadlock
	}
	return sc.opDoneChans[commandIdx]
}

// applier keeps receiving applied logs sent from raft, and handle them one by one.
func (sc *ShardCtrler) applier() {
	for {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				sc.mu.Lock()
				// this op is outdated
				if op.SerialNum <= sc.clientId2SerialNum[op.ClientId] {
					DebugLog(dJoin, sc, "outdated SerialNum, op.SerialNum=%v, now=%v, op=%v",
						op.SerialNum, sc.clientId2SerialNum[op.ClientId], op.String())
					sc.mu.Unlock()
					continue
				}
				// check lastApplied index
				if msg.CommandIndex <= sc.lastApplied {
					DebugLog(dJoin, sc, "outdated commit")
					sc.mu.Unlock()
					continue
				}
				// update last applied and client's serial num
				sc.lastApplied = msg.CommandIndex
				sc.clientId2SerialNum[op.ClientId] = op.SerialNum

				lastCfg := sc.configs[len(sc.configs)-1]
				switch op.OperationType {
				case JOIN:
					if len(sc.configs) == 1 {
						cfg := FirstJoin(JoinArgs{Servers: op.ServersJoined, ClientId: op.ClientId, SerialNum: op.SerialNum})
						sc.configs = append(sc.configs, cfg)
						DebugLog(dJoin, sc, "[APPLIER] join action done, cmdIdx=%v, len Cfgs=%v, => first join res=%v, allcfgs=%v",
							msg.CommandIndex, len(sc.configs), printGID2Shards(convertToNewG2S(cfg.Shards)), sc.printAllCfgs())
					} else {
						cfg := Join(lastCfg, JoinArgs{Servers: op.ServersJoined, ClientId: op.ClientId, SerialNum: op.SerialNum})
						sc.configs = append(sc.configs, cfg)
						DebugLog(dJoin, sc, "[APPLIER] join action done, cmdIdx=%v, len Cfgs=%v, => ori=%v, now=%v, allcfgs=%v",
							msg.CommandIndex, len(sc.configs),
							printGID2Shards(convertToNewG2S(lastCfg.Shards)), printGID2Shards(convertToNewG2S(cfg.Shards)), sc.printAllCfgs())
					}
					break
				case LEAVE:
					if len(sc.configs) == 1 {
						//op.ErrMsg = "can not remove GID since no config exists"
						//panic("can not remove GID since no config exists")
						DebugLog(dLeave, sc, "can not remove GID since no config exists")
					} else {
						cfg := Leave(lastCfg, LeaveArgs{GIDs: op.GidLeaved, ClientId: op.ClientId, SerialNum: op.SerialNum})
						sc.configs = append(sc.configs, cfg)
						DebugLog(dLeave, sc, "[APPLIER] leave action done, cmdIdx=%v, len Cfgs=%v, => ori=%v, now=%v, allcfgs=%v",
							msg.CommandIndex, len(sc.configs),
							printGID2Shards(convertToNewG2S(lastCfg.Shards)), printGID2Shards(convertToNewG2S(cfg.Shards)), sc.printAllCfgs())
					}
					break
				case MOVE:
					if len(sc.configs) == 1 {
						//op.ErrMsg = "can not move shards since no config exists"
						panic("can not move shards since no config exists")
					} else {
						sc.configs = append(sc.configs, Move(lastCfg, MoveArgs{
							Shard:    op.ShardMoved,
							GID:      op.GidMovedTo,
							ClientId: op.ClientId, SerialNum: op.SerialNum,
						}))
					}
					break
				case QUERY:
					var targetCfg Config
					for _, cfg := range sc.configs {
						targetCfg = cfg
						// if the desire config is found, return
						if cfg.Num == op.QueryNum {
							break
						}
					}
					op.QueryLenCfg = len(sc.configs)
					op.QueryRes = Config{
						Num:    targetCfg.Num,
						Shards: targetCfg.Shards,
						Groups: targetCfg.Groups,
					}
					DebugLog(dQuery, sc, "[APPLIER] query action done, cmdIdx=%v, allcfgs=%v", msg.CommandIndex, sc.printAllCfgs())
					break
				default:
					panic(fmt.Sprintf("unexpected op type=%v", op.OperationType))
				}
				opDoneChan := sc.getOpDoneChan(msg.CommandIndex)
				opDoneChan <- op // notifier the function who is waiting
				sc.mu.Unlock()

			} else {
				panic("snapshot is not supported in shard controller")
			}
		}
	}
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
		ClientId:      args.ClientId,
		SerialNum:     args.SerialNum,
		OperationType: JOIN,
		ServersJoined: args.Servers,
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

// The Leave RPC's argument is a list of GIDs of previously joined groups.
// The shardctrler should create a new configuration that does not include those groups,
// and that assigns those groups' shards to the remaining groups.
// The new configuration should divide the shards as evenly as possible among the groups,
// and should move as few shards as possible to achieve that goal.
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
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
		ClientId:      args.ClientId,
		SerialNum:     args.SerialNum,
		OperationType: LEAVE,
		GidLeaved:     args.GIDs,
	}
	// Start an agreement on this op.
	commandIdx, _, _ := sc.rf.Start(op)
	opDoneChan := sc.getOpDoneChan(commandIdx)
	sc.mu.Unlock()

	// Wait until this agreement is applied (timeout is introduced).
	select {
	case doneOp := <-opDoneChan:
		if doneOp.SerialNum == args.SerialNum && doneOp.ClientId == args.ClientId {
			reply.Err = doneOp.ErrMsg
			reply.WrongLeader = false
			DebugLog(dLeave, sc, "leave done, SerialNum=%v", doneOp.SerialNum)
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

// The Move RPC's arguments are a shard number and a GID.
// The shardctrler should create a new configuration in which the shard is assigned to the group.
// The purpose of Move is to allow us to test your software.
// A Join or Leave following a Move will likely un-do the Move, since Join and Leave re-balance.
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
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
		ClientId:      args.ClientId,
		SerialNum:     args.SerialNum,
		OperationType: MOVE,
		ShardMoved:    args.Shard,
		GidMovedTo:    args.GID,
	}
	// Start an agreement on this op.
	commandIdx, _, _ := sc.rf.Start(op)
	opDoneChan := sc.getOpDoneChan(commandIdx)
	sc.mu.Unlock()

	// Wait until this agreement is applied (timeout is introduced).
	select {
	case doneOp := <-opDoneChan:
		if doneOp.SerialNum == args.SerialNum && doneOp.ClientId == args.ClientId {
			reply.Err = doneOp.ErrMsg
			reply.WrongLeader = false
			DebugLog(dMove, sc, "move done, SerialNum=%v", doneOp.SerialNum)
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

// The Query RPC's argument is a configuration number.
// The shardctrler replies with the configuration that has that number.
// If the number is -1 or bigger than the biggest known configuration number,
// the shardctrler should reply with the latest configuration.
// The result of Query(-1) should reflect every Join, Leave, or Move RPC that the shardctrler finished handling before it received the Query(-1) RPC.
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
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
		DebugLog(dQuery, sc, "this is an outdated or duplicated request")
		reply.WrongLeader = false
		sc.mu.Unlock()
		return
	}

	// create a unique identifier for this operation
	op := Op{
		ClientId:      args.ClientId,
		SerialNum:     args.SerialNum,
		OperationType: QUERY,
		QueryNum:      args.Num,
	}
	// Start an agreement on this op.
	commandIdx, _, _ := sc.rf.Start(op)
	opDoneChan := sc.getOpDoneChan(commandIdx)
	sc.mu.Unlock()
	DebugLog(dQuery, sc, "start new agreement, cmdIdx=%v", commandIdx)
	// Wait until this agreement is applied (timeout is introduced).
	select {
	case doneOp := <-opDoneChan:
		if doneOp.SerialNum == args.SerialNum && doneOp.ClientId == args.ClientId {
			reply.Err = doneOp.ErrMsg
			reply.WrongLeader = false
			reply.Config = doneOp.QueryRes
			DebugLog(dQuery, sc, "query(%v) done, len=%v, rescfg=%v", args.Num, doneOp.QueryLenCfg, doneOp.QueryRes)
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
