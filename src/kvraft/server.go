package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const CmdTimeout = 200 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	OpType    string
	SerialNum int
	ClientId  int64
}

func (op Op) toString() string {
	res := fmt.Sprintf("key=%v, value=%v, opType=%v, seqNum=%v, clientId=%v", op.Key, op.Value, op.OpType, op.SerialNum, op.ClientId)
	return res
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	lastApplied        int
	inMemoryDB         map[string]string // in-memory kv map
	clientId2SerialNum map[int64]int     // the latest sequence number of each client
	logIdx2chan        map[int]chan Op   // Once raft applied the command, notification is sent by this chan
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

// getResponseChan
//
//	@Description:
//	@receiver kv
//	@param clientId
//	@return chan
func (kv *KVServer) getResponseChan(commandIdx int) chan Op {
	if _, hasKey := kv.logIdx2chan[commandIdx]; !hasKey {
		kv.logIdx2chan[commandIdx] = make(chan Op, 1) // buf size need to be 1 in case new leader to commit previous logs
	}
	return kv.logIdx2chan[commandIdx]
}

func (kv *KVServer) CommandHandler(req *CommandRequest, rsp *CommandResponse) {
	rsp.Err = OK
	rsp.LeaderId = kv.me
	if kv.killed() {
		rsp.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		rsp.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()

	// check if the serial number has already been executed
	if req.OpType != GET && kv.clientId2SerialNum[req.ClientId] >= req.SerialNum {
		kv.mu.Unlock()
		return
	}

	// start an agreement
	// Note: for get request, "leader should exchange heartbeat messages with a majority of
	//       the cluster before responding to read-only requests"
	//       So it is also necessary to start this agreement for get request
	op := Op{
		Key:       req.Key,
		Value:     req.Value,
		OpType:    req.OpType,
		SerialNum: req.SerialNum,
		ClientId:  req.ClientId,
	}
	commandIdx, _, _ := kv.rf.Start(op)
	responseChan := kv.getResponseChan(commandIdx)
	kv.mu.Unlock()

	DebugLog(dClient, "S%v (KV); Start() success; transId=%v req={%v}", kv.me, req.TransId, req.toString())
	// wait until Raft applies this command

	select {
	case msg := <-responseChan:
		DebugLog(dClient, "S%v (KV); ResponseChan notified", kv.me)
		if msg.SerialNum == req.SerialNum && msg.ClientId == req.ClientId /*todo why?*/ {
			if req.OpType == GET {
				kv.mu.Lock()
				val, ok := kv.inMemoryDB[req.Key]
				if ok {
					rsp.Value = val
				} else {
					rsp.Value = ""
					rsp.Err = ErrNoKey
				}
				kv.mu.Unlock()
			}
		} else {
			rsp.Err = ErrTimeout
		}
	case <-time.After(CmdTimeout):
		DebugLog(dClient, "S%v (KV); timeout; req={%v}", kv.me, req.toString())
		rsp.Err = ErrTimeout
	}
	go func() {
		kv.mu.Lock()
		delete(kv.logIdx2chan, commandIdx)
		kv.mu.Unlock()
	}()

}

func (kv *KVServer) dbToString() string {
	res := ""
	for key, value := range kv.inMemoryDB {
		s := fmt.Sprintf("{K: %v, V: %v}", key, value)
		res += s
	}
	return "[" + res + "]"
}

func (kv *KVServer) applier() {
	for {
		select {
		case msg := <-kv.applyCh:
			noop, isNoop := msg.Command.(string)
			if isNoop && noop == NOOP {
				DebugLog(dClient, "S%v (KV); no-op skip", kv.me)
				continue
			}
			if msg.CommandValid {
				op := msg.Command.(Op)
				DebugLog(dClient, "S%v (KV); applier notified, msg={cmdIdx=%v, cmdOk=%v, op=}", kv.me, msg.CommandIndex, msg.CommandValid, op.toString())
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex
				responseChan := kv.getResponseChan(msg.CommandIndex)
				if op.SerialNum > kv.clientId2SerialNum[op.ClientId] {
					if op.OpType == PUT {
						DebugLog(dClient, "S%v (KV); put[%v]=%v", kv.me, op.Key, op.Value)
						kv.inMemoryDB[op.Key] = op.Value
					}
					if op.OpType == APPEND {
						DebugLog(dClient, "S%v (KV); app[%v]+=%v", kv.me, op.Key, op.Value)
						kv.inMemoryDB[op.Key] += op.Value
					}
					kv.clientId2SerialNum[op.ClientId] = op.SerialNum
				}
				DebugLog(dClient, "S%v (KV); currDB=%v", kv.me, kv.dbToString())
				kv.mu.Unlock()
				responseChan <- op
			}

		}
	}
}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.inMemoryDB = make(map[string]string)
	kv.logIdx2chan = make(map[int]chan Op)
	kv.clientId2SerialNum = make(map[int64]int)
	kv.lastApplied = 0
	go func() {
		for {
			kv.mu.Lock()
			DebugLog(dClient, "S%v (KV); aaaalive!!!!", kv.me)
			kv.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()
	go kv.applier()
	return kv
}
