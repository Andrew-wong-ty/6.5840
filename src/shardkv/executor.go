package shardkv

import (
	"6.5840/shardctrler"
	"fmt"
	"strconv"
)

// applier keeps receiving applied logs sent from raft, and handle them one by one.
func (kv *ShardKV) applier() {
	for {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid && msg.Command != nil {
				op := msg.Command.(Op)
				kv.mu.Lock()

				// check lastApplied index
				if msg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex

				// check if op is repeated/outdated
				if !op.Idempotent && op.SerialNum <= kv.clientId2SerialNum[op.ClientId] {
					DebugLog(dApply, kv, "op=%v, op.SerialNum <= kv.clientId2SerialNum[op.ClientId]; continue", op.OpType)
					kv.mu.Unlock()
					continue
				}

				switch op.OpType {
				case UPDATECONFIG:
					op.Error = kv.doUpdateConfig(&op)
				case INSTALLSHARD:
					op.Error, op.InstalledSuccessShards = kv.doInstallShard(&op)
				case DELETESHARD:
					op.Error = kv.doDeleteShard(&op)
				case GET:
					op.ResultForGet, op.Error = kv.doGet(&op)
				case PUT:
					op.Error = kv.doPut(&op)
				case APPEND:
					op.Error = kv.doAppend(&op)
				default:
					panic(fmt.Sprintf("unexpected op type: '%v'", op.OpType))
				}

				if !op.Idempotent && op.Error == OK {
					// only update serialNum after it is executed successfully
					kv.clientId2SerialNum[op.ClientId] = op.SerialNum
				}

				// notify Get/PutAppend function that this operation is done, and send results by op
				opDoneChan := kv.getOpDoneChan(msg.CommandIndex)
				opDoneChan <- op
				// do snapshot
				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
					kv.rf.Snapshot(msg.CommandIndex, kv.encodeSnapshot())
					DebugLog(dSnap, kv, "do snapshot done, currCfg.Num=%v db=%v", kv.currCfg.Num, db2str(kv.inMemoryDB))
				}
				kv.mu.Unlock()
			}

			if msg.SnapshotValid {
				kv.mu.Lock()
				kv.decodeAndInstallSnapshot(msg.Snapshot, msg.SnapshotIndex)
				kv.mu.Unlock()
			}
		}
	}
}

// getOpDoneChan returns an op-done-notification-chan for a given commandIdx
func (kv *ShardKV) getOpDoneChan(commandIdx int) chan Op {
	if _, hasKey := kv.opDoneChans[commandIdx]; !hasKey {
		kv.opDoneChans[commandIdx] = make(chan Op, 8)
	}
	return kv.opDoneChans[commandIdx]
}

func (kv *ShardKV) deleteKeyFromOpDoneChans(cmdIdx int) {
	kv.mu.Lock()
	delete(kv.opDoneChans, cmdIdx)
	kv.mu.Unlock()
}

func (kv *ShardKV) isShardNotReady(key string) bool {
	shardId := key2shard(key)
	newShard := kv.currCfg.Shards[shardId] == kv.gid && kv.prevCfg.Shards[shardId] != kv.gid
	outdated := kv.inMemoryDB[shardId].Version < kv.currCfg.Num
	return newShard && outdated
}

func (kv *ShardKV) isKeyReady(op *Op) (bool, Err) {
	responsible := kv.isCurrCfgResponsibleForKey(op.Key)
	shardNotReady := kv.isShardNotReady(op.Key)
	if !responsible {
		DebugLog(dApply, kv, "op= %v key=%v (shard=%v); not responsible", op.OpType, op.Key, key2shard(op.Key))
		return false, ErrWrongGroup
	} else if shardNotReady {
		DebugLog(dApply, kv, "op= %v key=%v (shard=%v); not ready; DB=%v, currFbg=%v", op.OpType, op.Key, key2shard(op.Key), db2str(kv.inMemoryDB), kv.currCfg.String())
		return false, ErrShardNotReady
	} else {
		// the machine is responsible for the key's shard and the shard is ready
		return true, OK
	}
}

// return the value of a key from the DB;
// if key non-exist, return an error msf
func (kv *ShardKV) doGet(op *Op) (string, Err) {
	keyReady, err := kv.isKeyReady(op)
	if keyReady {
		sid := key2shard(op.Key)
		value, exist := kv.inMemoryDB[sid].Data[op.Key]
		if !exist {
			DebugLog(dGet, kv, "get %v, Err=%v; applied", op.Key, ErrNoKey)
			return "", ErrNoKey
		} else {
			DebugLog(dGet, kv, "get %v = %v, Err=%v; db=%v, client=%v, serialNum=%v applied", op.Key, OK, kv.inMemoryDB, op.ClientId, op.SerialNum, value)
			return value, OK
		}
	} else {
		return "", err
	}
}

func (kv *ShardKV) doPut(op *Op) Err {
	keyReady, err := kv.isKeyReady(op)
	if keyReady {
		sid := key2shard(op.Key)
		kv.inMemoryDB[sid].Data[op.Key] = op.Value
		DebugLog(dPut, kv, "put %v = %v applied", op.Key, op.Value)
		return OK
	} else {
		return err
	}

}

func (kv *ShardKV) doAppend(op *Op) Err {
	keyReady, err := kv.isKeyReady(op)
	if keyReady {
		sid := key2shard(op.Key)
		kv.inMemoryDB[sid].Data[op.Key] += op.Value
		DebugLog(dAppend, kv, "append %v -> += %v => %v applied", op.Key, op.Value, kv.inMemoryDB[sid].Data[op.Key])
		return OK
	} else {
		return err
	}
}

func (kv *ShardKV) doUpdateConfig(op *Op) Err {
	newCfg := decodeConfig(op.NewConfig)
	if kv.currCfg.Num >= newCfg.Num {
		DebugLog(dApply, kv, "no update cfg since currCfg.Num %v >= newCfg.Num %v", kv.currCfg.Num, newCfg.Num)
		return ErrOutdatedConfig
	}
	kv.prevCfg = shardctrler.DeepCopyConfig(kv.currCfg)
	kv.currCfg = newCfg
	// handle the case that this is the last group to leave.
	dbgMsg := ""
	if len(kv.currCfg.Groups) == 0 {
		for shardId := 0; shardId < shardctrler.NShards; shardId++ {
			if kv.prevCfg.Shards[shardId] == kv.gid {
				// update data version and clear all data (it doesn't need to send these to anyone)
				kv.inMemoryDB[shardId].Version = kv.currCfg.Num
				kv.inMemoryDB[shardId].Data = make(map[string]string)
				dbgMsg += strconv.Itoa(shardId) + " "
			}
		}
	}
	if dbgMsg != "" {
		DebugLog(dSend, kv, "final leave, clear DB; shards=%v", dbgMsg)
		dbgMsg = ""
	}
	// handle the case that this is the first group arrives
	if len(kv.prevCfg.Groups) == 0 {
		for shardId := 0; shardId < shardctrler.NShards; shardId++ {
			if kv.currCfg.Shards[shardId] == kv.gid {
				kv.inMemoryDB[shardId].Version = kv.currCfg.Num // mark all shards to be up-to-date
				dbgMsg += fmt.Sprintf("S[%v].v=%v ", shardId, kv.inMemoryDB[shardId].Version)
			}
		}
	}
	if dbgMsg != "" {
		DebugLog(dSend, kv, "the first cfg arrives, update DB.version => %v", dbgMsg)
		dbgMsg = ""
	}
	// update the version of shards that the server is still holding
	for shardId := 0; shardId < shardctrler.NShards; shardId++ {
		if kv.currCfg.Shards[shardId] == kv.gid && kv.prevCfg.Shards[shardId] == kv.gid {
			kv.inMemoryDB[shardId].Version = kv.currCfg.Num
			dbgMsg += strconv.Itoa(shardId) + " "
		}
	}
	if dbgMsg != "" {
		DebugLog(dSend, kv, "the server holds %v in both prev and curr Cfg, update Version to be %v", dbgMsg, kv.currCfg.Num)
	}

	DebugLog(dApply, kv, "updateCfg success, currCfg=%v; DB=%v", kv.currCfg.Num, db2str(kv.inMemoryDB))
	return OK
}

// Install the shardData; it first checks if the data is outdated (smaller version)
// if data is outdated, reject install and return OK; else install and update DB's shard data's version
func (kv *ShardKV) doInstallShard(op *Op) (Err, string) {
	shardDatas := decodeDB(op.ShardData) // shardData
	shardIDs := decodeSlice(op.ShardIDs) // shardIDs to be installed
	version := op.ShardDataVersion
	successInstalledShardIDs := make([]int, 0) // what shards are installed finally
	clientId2SerialNum := decodeMap(op.Client2SerialNum)
	// install the new shards, ignore the outdated ones.
	for _, shardId := range shardIDs {
		shardData := shardDatas[shardId]
		if version > kv.inMemoryDB[shardId].Version { // only install the new ones
			successInstalledShardIDs = append(successInstalledShardIDs, shardId)
			// install
			kv.inMemoryDB[shardId].Version = version
			kv.inMemoryDB[shardId].Data = shardData
		}
	}
	// avoid the same Put/Append being executed twice because of shard migration
	for clientId, seqId := range clientId2SerialNum {
		if r, ok := kv.clientId2SerialNum[clientId]; !ok || r < seqId {
			kv.clientId2SerialNum[clientId] = seqId
		}
	}
	DebugLog(dReceive, kv, "version=%v, install shards: %v; data=%v; success: %v; DB=%v applied",
		version, shardIDs, shardDatas, successInstalledShardIDs, db2str(kv.inMemoryDB))
	return OK, encodeSlice(successInstalledShardIDs)
}

func (kv *ShardKV) doDeleteShard(op *Op) Err {
	version := op.Version
	if op.ShardIDs == "" {
		DebugLog(dSend, kv, "warning, serializedShardIDs is empty")
		return OK
	}
	shardIDs := decodeSlice(op.ShardIDs)
	successDeletedShardIDs := make([]int, 0)
	for _, shardID := range shardIDs {
		if version > kv.inMemoryDB[shardID].Version {
			successDeletedShardIDs = append(successDeletedShardIDs, shardID)
			kv.inMemoryDB[shardID].Version = version
			kv.inMemoryDB[shardID].Data = nil
		}
	}
	DebugLog(dSend, kv, "version=%v, deleted shards: %v, success: %v; DB=%v; applied", version, shardIDs, successDeletedShardIDs, db2str(kv.inMemoryDB))
	return OK
}

func (kv *ShardKV) isCurrCfgResponsibleForKey(key string) bool {
	sid := key2shard(key)
	expectGID := kv.currCfg.Shards[sid]
	return kv.gid == expectGID
}
