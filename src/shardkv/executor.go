package shardkv

import (
	"6.5840/shardctrler"
	"fmt"
	"strconv"
)

// getOpDoneChan returns an op-done-notification-chan for a given commandIdx
func (kv *ShardKV) getOpDoneChan(commandIdx int) chan Op {
	if _, hasKey := kv.opDoneChans[commandIdx]; !hasKey {
		kv.opDoneChans[commandIdx] = make(chan Op, 8) //! Note: must be unbuffered to avoid deadlock
	}
	return kv.opDoneChans[commandIdx]
}

func (kv *ShardKV) isKeyNotReady(key string) bool {
	shardId := key2shard(key)
	newShard := kv.currCfg.Shards[shardId] == kv.gid && kv.prevCfg.Shards[shardId] != kv.gid
	outdated := kv.inMemoryDB[shardId].Version < kv.currCfg.Num
	return newShard && outdated
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
					DebugLog(dApply, kv, "op=%v, msg.CommandIndex <= kv.lastApplied; continue", op.OpType)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex

				// TODO: refactor
				// check if op is repeated/outdated
				if op.OpType != GET && op.OpType != INSTALLSHARD { // GET and INSTALLSHARD (only update new shards) are idempotent
					if op.SerialNum <= kv.clientId2SerialNum[op.ClientId] {
						DebugLog(dApply, kv, "op=%v, op.SerialNum <= kv.clientId2SerialNum[op.ClientId]; continue", op.OpType)
						kv.mu.Unlock()
						continue
					}
				}

				if op.OpType == UPDATECONFIG {
					// update curr and prev config
					op.Error = kv.doUpdateConfig(op.NewConfig) // TODO: refactor to pass op pointer
				} else if op.OpType == INSTALLSHARD {
					// put migrated shards into currDB
					op.Error, op.InstalledSuccessShards = kv.doInstallShard(op.ShardData, op.ShardIDs, op.ShardDataVersion, op.Client2SerialNum)
				} else if op.OpType == DELETESHARD {
					op.Error = kv.doDeleteShard(op.ShardIDs, op.Version)
				} else {
					// check if this server is responsible for this key; if yes, whether shard is ready
					responsible := kv.isCfgResponsibleForKey(kv.currCfg, op.Key)
					if responsible {
						shardNotReady := kv.isKeyNotReady(op.Key)
						if shardNotReady {
							op.Error = ErrShardNotReady
							DebugLog(dApply, kv, "op= %v key=%v (shard=%v); not ready; DB=%v, currFbg=%v",
								op.OpType, op.Key, key2shard(op.Key), db2str(kv.inMemoryDB), kv.currCfg.String())
						} else {
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
						}
					} else {
						DebugLog(dApply, kv, "op= %v key=%v (shard=%v); not responsible", op.OpType, op.Key, key2shard(op.Key))
						op.Error = ErrWrongGroup
					}

				}
				// TODO: refactor this
				if op.OpType != GET && op.OpType != INSTALLSHARD {
					if op.OpType == PUT || op.OpType == APPEND { // for put and append, only update serialNum after it is executed
						if op.Error == OK {
							kv.clientId2SerialNum[op.ClientId] = op.SerialNum
						}
					} else {
						// INSTALLSHARD /  internal=>  UPDATECONFIG  DELETESHARD
						kv.clientId2SerialNum[op.ClientId] = op.SerialNum
					}
				}
				// notify Get/PutAppend function that this operation is done, and send results by op
				opDoneChan := kv.getOpDoneChan(msg.CommandIndex)
				opDoneChan <- op //! potential deadlock!!!!!
				// do snapshot
				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
					go kv.rf.Snapshot(msg.CommandIndex, kv.encodeSnapshot())
				}
				kv.mu.Unlock()
			}
			// a raft snapshot is installed on this machine
			if msg.SnapshotValid {
				kv.mu.Lock()
				kv.decodeAndInstallSnapshot(msg.Snapshot, msg.SnapshotIndex)
				kv.mu.Unlock()
			}
		}
	}
}

// return the value of a key from the DB;
// if key non-exist, return an error msf
// ! should be in lock context (kv.mu)
func (kv *ShardKV) doGet(key string) (string, Err) {
	sid := key2shard(key)
	value, exist := kv.inMemoryDB[sid].Data[key]
	if !exist {
		DebugLog(dGet, kv, "get %v, Err=%v; applied", key, ErrNoKey)
		return "", ErrNoKey
	} else {
		DebugLog(dGet, kv, "get %v = %v, Err=%v; applied", key, value, OK)
		return value, OK
	}
}

// ! should be in lock context (kv.mu)
func (kv *ShardKV) doPut(key, value string) Err {
	sid := key2shard(key)
	kv.inMemoryDB[sid].Data[key] = value
	DebugLog(dPut, kv, "put %v = %v applied", key, value)
	return OK
}

// ! should be in lock context (kv.mu)
func (kv *ShardKV) doAppend(key, value string) Err {
	sid := key2shard(key)
	kv.inMemoryDB[sid].Data[key] += value
	DebugLog(dAppend, kv, "append %v -> += %v => %v applied", key, value, kv.inMemoryDB[sid].Data[key])
	return OK
}

func (kv *ShardKV) doUpdateConfig(serializedCfg string) Err {
	newCfg := decodeConfig(serializedCfg)
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
				kv.inMemoryDB[shardId].Version = kv.currCfg.Num
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

// ! should be in lock context (kv.mu)
// Install the shardData; it first checks if the data is outdated (smaller version)
// if data is outdated, reject install and return OK; else install and update DB's shard data's version
func (kv *ShardKV) doInstallShard(serializedData string, serializedShardIDs string, version int, client2Seq string) (Err, string) {
	shardDatas := decodeDB(serializedData)      // shardData
	shardIDs := decodeSlice(serializedShardIDs) // shardIDs to be installed
	successInstalledShardIDs := make([]int, 0)  // what shards are installed finally
	clientId2SerialNum := decodeMap(client2Seq)
	// install the new shards, ignore the outdated ones.
	for _, shardId := range shardIDs {
		shardData := shardDatas[shardId]
		if version > kv.inMemoryDB[shardId].Version { // only install the new ones
			successInstalledShardIDs = append(successInstalledShardIDs, shardId)
			// install
			kv.inMemoryDB[shardId].Version = version
			kv.inMemoryDB[shardId].Data = shardData
		}
		// reject install a smaller-version shard data
	}
	/*
		To prevent the same put/append request to be applied twice
		For example, client send Append(Key="1", val="2") to 101, suppose ErrTimeout happens and the PutAppend RPC returns.
		Suppose later this Append applied (now in the DB, key"1"="2"). After PutAppend RPC returns, the client send Append again.
		Suppose now reconfig happens, group 102 is responsible for this key.
		Next, the client request for 101 group for Append(Key="1", val="2") but get rejected by ErrWrongGroup.
		Then client queries the latest config and retires, which sends  Append(Key="1", val="2") to 102.  (Suppose group 101 has migrated the shard contains `key"1"="2"` to 102 before this request.)
		In the PutAppend RPC on group 102, suppose the SerialNum check pass, and finally Append(Key="1", val="2") is applied on 102.
		At this time key"1"="22" in DB.

		So in this case, Append(Key="1", val="2") is applied twice, which is buggy in the same client Append request.
		Therefore, copying the clientId2SerialNum from 101 to 102 when shard migrate can solve it.
	*/

	for clientId, seqId := range clientId2SerialNum {
		if r, ok := kv.clientId2SerialNum[clientId]; !ok || r < seqId {
			kv.clientId2SerialNum[clientId] = seqId
		}
	}
	DebugLog(dReceive, kv, "version=%v, install shards: %v; data=%v; success: %v; DB=%v applied",
		version, shardIDs, shardDatas, successInstalledShardIDs, db2str(kv.inMemoryDB))
	return OK, encodeSlice(successInstalledShardIDs)
}

func (kv *ShardKV) doDeleteShard(serializedShardIDs string, version int) Err {
	shardIDs := decodeSlice(serializedShardIDs)
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
