package shardctrler

import "fmt"

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
						cfg := FirstJoin(JoinArgs{Servers: deserializeServersJoined(op.SerializedServersJoined), ClientId: op.ClientId, SerialNum: op.SerialNum})
						sc.configs = append(sc.configs, cfg)
						DebugLog(dJoin, sc, "[APPLIER] join action done, cmdIdx=%v, len Cfgs=%v, => first join res=%v, allcfgs=%v",
							msg.CommandIndex, len(sc.configs), printGID2Shards(convertToNewG2S(cfg.Shards)), sc.printAllCfgs())
					} else {
						cfg := Join(lastCfg, JoinArgs{Servers: deserializeServersJoined(op.SerializedServersJoined), ClientId: op.ClientId, SerialNum: op.SerialNum})
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
						cfg := Leave(lastCfg, LeaveArgs{GIDs: deserializeGidLeaved(op.SerializedGidLeaved), ClientId: op.ClientId, SerialNum: op.SerialNum})
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
					op.SerializedQueryRes = serialize(Config{
						Num:    targetCfg.Num,
						Shards: targetCfg.Shards,
						Groups: targetCfg.Groups,
					})
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
