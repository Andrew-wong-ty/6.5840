package shardkv

import (
	"6.5840/shardctrler"
	"sort"
)

// ! use in kv.mu context
func (kv *ShardKV) getUnreceivedShards() []int {
	nonReadyShards := make([]int, 0)
	if kv.currCfg.Num == 0 || kv.prevCfg.Num == 0 {
		DebugLog(dReceive, kv, "(getUnreceivedShards) prev_is_0=%v, curr_is_0=%v => all received", kv.prevCfg.Num == 0, kv.currCfg.Num == 0)
		return nonReadyShards
	}
	// if a shard's version is smaller than the current config's Num, it is not ready
	for shardId, shard := range kv.inMemoryDB {
		isNewShard := kv.currCfg.Shards[shardId] == kv.gid && kv.prevCfg.Shards[shardId] != kv.gid
		if isNewShard && shard.Version < kv.currCfg.Num {
			nonReadyShards = append(nonReadyShards, shardId)
		}
	}
	return nonReadyShards
}

// ! use in kv.mu context
func (kv *ShardKV) getUnsentShards() map[int][]int {
	gid2UnsentShards := make(map[int][]int)
	for shardId, shard := range kv.inMemoryDB {
		isRemovedShard := kv.prevCfg.Shards[shardId] == kv.gid && kv.currCfg.Shards[shardId] != kv.gid
		if isRemovedShard && shard.Version < kv.currCfg.Num {
			gid := kv.currCfg.Shards[shardId]
			gid2UnsentShards[gid] = append(gid2UnsentShards[gid], shardId)
		}
	}
	return gid2UnsentShards
}

// start several goroutines to send  ShardKV.InstallShardData RPC to install shards on group
// ! should be in lock context
func (kv *ShardKV) sendRemovedShards(gid2UnsentShards map[int][]int) {
	currCfg := shardctrler.DeepCopyConfig(kv.currCfg)
	for gid, shardIDs := range gid2UnsentShards {
		// initialize
		var shardDB [shardctrler.NShards]map[string]string
		// copy shard data from current DB
		for _, sid := range shardIDs {
			shardDB[sid] = deepCopyMapStr2Str(kv.inMemoryDB[sid].Data)
		}
		// send install shard RPC
		args := InstallShardArgs{
			ToGid:            gid,
			Data:             encodeDB(shardDB),
			ShardIDs:         encodeSlice(shardIDs),
			Client2SerialNum: encodeMap(kv.clientId2SerialNum),
			CfgNum:           kv.currCfg.Num,
			ClientID:         kv.clientId,
		}
		go func() {
			DebugLog(dMigrate, kv, "sending shardData, cfgUsed=%v, toGid=%v, shardIDs=%v, data=%v", currCfg.String(), args.ToGid, decodeSlice(args.ShardIDs), shardDB)
			if ok, successInstalledShards := kv.sendInstallShardData(currCfg, &args); ok {
				DebugLog(dMigrate, kv, "send shardData SUCCESS!, expectInstall=%v; now start delete agreement", decodeSlice(args.ShardIDs))
				// start agreement to delete shards
				kv.rf.Start(Op{
					OpType:    DELETESHARD,
					ShardIDs:  successInstalledShards, // shards to be deleted
					SerialNum: uint64(currCfg.Num),
					Version:   currCfg.Num,
					ClientId:  kv.clientId + 1, // 1 to distinguish with UPDATECONFIG
				})
			}
		}()
	}
}

// sends RPC to shardctrler for the latest Config
// Once it gets the reply, it updates the kv server's config
func (kv *ShardKV) pollConfig() {
	if kv.killed() {
		return
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()

	currCfg := shardctrler.DeepCopyConfig(kv.currCfg)

	if gid2UnsentShards := kv.getUnsentShards(); len(gid2UnsentShards) != 0 {
		DebugLog(dSend, kv, "sending shards (gid->sids) %v", gid2UnsentShards)
		kv.sendRemovedShards(gid2UnsentShards)
	}
	if unreceivedShards := kv.getUnreceivedShards(); len(unreceivedShards) != 0 {
		DebugLog(dReceive, kv, "waiting shards %v, prevCfg=%v, currCfg=%v, ", unreceivedShards, kv.prevCfg.String(), kv.currCfg.String())
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()
	// request the next config
	nextCfg := kv.scc.Query(currCfg.Num + 1)
	if nextCfg.Num == currCfg.Num+1 {
		DebugLog(dReceive, kv, "new Cfg polled, nextCfg=%v, currCfg=%v; start agreement to update cfg", nextCfg.String(), currCfg.String())
		// update config
		kv.rf.Start(Op{
			OpType:    UPDATECONFIG,
			NewConfig: encodeConfig(nextCfg),
			SerialNum: uint64(nextCfg.Num),
			ClientId:  kv.clientId,
		})
	}
}

// ticker handles tick emission and do correspondent actions
func (kv *ShardKV) ticker() {
	for !kv.killed() {
		select {
		case <-kv.cfgPollingTicker.C:
			go kv.pollConfig()
		}
	}
}

/******************** Helper functions *********************/

func diffSets(prev, curr map[int]bool) (added, removed []int) {
	// Find elements added to curr
	for elem := range curr {
		if !prev[elem] {
			added = append(added, elem)
		}
	}
	// Find elements removed from prev
	for elem := range prev {
		if !curr[elem] {
			removed = append(removed, elem)
		}
	}
	// Sort the slices
	sort.Ints(added)
	sort.Ints(removed)

	return added, removed
}

// deepCopyMapStr2Str creates a deep copy of a map[string]string.
func deepCopyMapStr2Str(original map[string]string) map[string]string {
	// Create a new map with the same capacity as the original map.
	cpy := make(map[string]string, len(original))

	// Copy each element from the original map to the new map.
	for key, value := range original {
		cpy[key] = value
	}

	return cpy
}

//// reconfigHandler keeps receiving notification about reconfiguration, handles one by a time
//func (kv *ShardKV) reconfigHandler() {
//	for {
//		select {
//		case msg := <-kv.reconfigChan:
//			if len(msg.addedShards) != 0 {
//				if len(msg.prevCfg.Groups) == 0 {
//					DebugLog(dMigrate, kv, "nobody will send %v shard, skip mark", msg.addedShards)
//				} else {
//					version := msg.currCfg.Num
//					kv.serialNum++
//					go kv.markNotReady(msg.addedShards, version, kv.serialNum) //? what if a get comes before it?
//				}
//
//			}
//
//			if len(msg.gid2RemovedShards) != 0 {
//				// send shards that are going to be removed from this machine to Leaders of replica group
//				DebugLog(dMigrate, kv, "gid2RemovedShards = %v; will be removed, migrate shard", msg.gid2RemovedShards)
//				for toGid, shardIDs := range msg.gid2RemovedShards {
//					// initialize
//					var shardDB [shardctrler.NShards]map[string]string
//					for i := 0; i < shardctrler.NShards; i++ {
//						shardDB[i] = make(map[string]string)
//					}
//					// copy shard data from current DB
//					kv.mu.Lock()
//					for _, sid := range shardIDs {
//						shardDB[sid] = deepCopyMapStr2Str(kv.inMemoryDB[sid].Data)
//					}
//					kv.mu.Unlock()
//					// send install shard RPC
//					kv.serialNum++
//					args := InstallShardArgs{
//						CfgUsed:   msg.currCfg,
//						ToGid:     toGid,
//						Data:      encodeDB(shardDB),
//						ShardIDs:  encodeSlice(shardIDs),
//						ClientID:  kv.clientId,
//						SerialNum: kv.serialNum,
//					}
//					go func() {
//						//time.Sleep(2 * time.Second) // TODO DELETE it !!!!!!!!!!
//						DebugLog(dMigrate, kv, "sending shardData, cfgUsed=%v, toGid=%v, shardIDs=%v, data=%v",
//							args.CfgUsed.String(), args.ToGid, decodeSlice(args.ShardIDs), shardDB)
//						if ok := kv.sendInstallShardData(&args); ok {
//							DebugLog(dMigrate, kv, "send shardData SUCCESS!, cfgUsed=%v, toGid=%v",
//								args.CfgUsed.String(), args.ToGid)
//							// TODO: GC
//						}
//					}()
//				}
//			}
//		}
//	}
//}

//// ! send msg to my self
//// mark addedShards are not ready
//func (kv *ShardKV) markNotReady(addedShards []int, version int, serialNum uint64) {
//	// since this machine is the leader, no leader check; no kill check; no cfg ready check
//	// also no SerialNum checks since this is done on the machine itself
//	kv.mu.Lock()
//	op := Op{
//		OpType:      UPDATECONFIG,
//		Version:     version,
//		AddedShards: encodeSlice(addedShards),
//		SerialNum:   serialNum,
//		ClientId:    kv.clientId,
//	}
//	commandIdx, _, _ := kv.rf.Start(op)
//	opDoneChan := kv.getOpDoneChan(commandIdx)
//	kv.mu.Unlock()
//	DebugLog(dMark, kv, "mark not ready {shards=%v, version=%v} started", addedShards, version)
//	// wait until response
//	select {
//	case <-opDoneChan:
//		// wait until done
//	}
//	go kv.deleteKeyFromOpDoneChans(commandIdx)
//}

//// getReconfigMsg calculates what shards are added and removed
//func (kv *ShardKV) getReconfigMsg(prev, curr shardctrler.Config) reconfigMsg {
//	prevShards := make(map[int]bool)
//	currShards := make(map[int]bool)
//	for sid, gid := range prev.Shards {
//		if gid == kv.gid {
//			prevShards[sid] = true
//		}
//	}
//	for sid, gid := range curr.Shards {
//		if gid == kv.gid {
//			currShards[sid] = true
//		}
//	}
//	addedShards, removedShards := diffSets(prevShards, currShards)
//	gid2RemovedShards := make(map[int][]int)
//	for _, sid := range removedShards {
//		receiverGID := curr.Shards[sid]
//		gid2RemovedShards[receiverGID] = append(gid2RemovedShards[receiverGID], sid)
//	}
//	return reconfigMsg{
//		prevCfg:           shardctrler.DeepCopyConfig(prev),
//		currCfg:           shardctrler.DeepCopyConfig(curr),
//		addedShards:       addedShards,
//		gid2RemovedShards: gid2RemovedShards,
//	}
//
//}

//// a struct for sending message to reconfigHandler for reconfig-handling
//type reconfigMsg struct {
//	prevCfg           shardctrler.Config
//	currCfg           shardctrler.Config
//	addedShards       []int         // shard id that to be added to this machine
//	gid2RemovedShards map[int][]int // receiver's GID -> removed shards to be sent from this machine
//}
//
//func (r *reconfigMsg) String() string {
//	return fmt.Sprintf("prevCfg=%v, currCfg=%v, addShard=%v, gid2removeShards=%v",
//		r.prevCfg.String(), r.currCfg.String(), r.addedShards, r.gid2RemovedShards)
//}
//
