package shardkv

import (
	"fmt"
)

// getOpDoneChan returns an op-done-notification-chan for a given commandIdx
func (kv *ShardKV) getOpDoneChan(commandIdx int) chan Op {
	if _, hasKey := kv.opDoneChans[commandIdx]; !hasKey {
		kv.opDoneChans[commandIdx] = make(chan Op, 1) //! Note: must be unbuffered to avoid deadlock
	}
	return kv.opDoneChans[commandIdx]
}

// applier keeps receiving applied logs sent from raft, and handle them one by one.
func (kv *ShardKV) applier() {
	for {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				kv.mu.Lock()
				// check lastApplied index
				if msg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex
				// check if op is repeated/outdated
				if op.SerialNum <= kv.clientId2SerialNum[op.ClientId] {
					kv.mu.Unlock()
					continue
				}
				kv.clientId2SerialNum[op.ClientId] = op.SerialNum
				// check if this server is responsible for this key
				kv.cfgMutex.Lock()
				responsible := kv.isCfgResponsibleForKey(kv.latestCfg, op.Key)
				kv.cfgMutex.Unlock()
				if responsible {
					if op.OpType == GET {
						// get value from map
						op.ResultForGet, op.Error = kv.doGet(op.Key)
					} else if op.OpType == PUT {
						// update k-v
						op.Error = kv.doPut(op.Key, op.Value)
					} else if op.OpType == APPEND {
						// add string on the key's value
						op.Error = kv.doAppend(op.Key, op.Value)
					} else {
						panic(fmt.Sprintf("unexpected op type:'%v'", op.OpType))
					}
				} else {
					op.Error = ErrWrongGroup
				}
				// notify Get/PutAppend function that this operation is done, and send results by op
				opDoneChan := kv.getOpDoneChan(msg.CommandIndex)
				opDoneChan <- op
				// do snapshot
				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
					go kv.rf.Snapshot(msg.CommandIndex, kv.encodeSnapshot())
				}
				kv.mu.Unlock()
			}
			// a raft snapshot is installed on this machine
			if msg.SnapshotValid {
				kv.mu.Lock()
				clt2SerialNum, db, cfg := kv.decodeSnapshot(msg.Snapshot)
				if msg.SnapshotIndex >= kv.lastApplied {
					kv.lastApplied = msg.SnapshotIndex
					kv.clientId2SerialNum = clt2SerialNum
					kv.inMemoryDB = db
					kv.cfgMutex.Lock()
					kv.latestCfg = cfg
					kv.cfgMutex.Unlock()
				}
				kv.mu.Unlock()
			}
		}
	}
}

// return the value of a key from the DB;
// if key non-exist, return an error msf
// ! should be in lock context
func (kv *ShardKV) doGet(key string) (string, Err) {
	value, exist := kv.inMemoryDB[key]
	if !exist {
		return "", ErrNoKey
	} else {
		return value, OK
	}
}

// ! should be in lock context
func (kv *ShardKV) doPut(key, value string) Err {
	kv.inMemoryDB[key] = value
	return OK
}

// ! should be in lock context
func (kv *ShardKV) doAppend(key, value string) Err {
	kv.inMemoryDB[key] += value
	return OK
}
