package shardctrler

import "fmt"

// applier keeps receiving applied logs sent from raft, and handle them one by one.
func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		select {
		case msg := <-sc.applyCh:
			if msg.Command != nil && msg.CommandValid {
				op := msg.Command.(Op)
				sc.mu.Lock()
				// this op is outdated
				if op.SerialNum <= sc.clientId2SerialNum[op.ClientId] {
					DebugLog(dJoin, sc, "outdated SerialNum, op.SerialNum=%v, now=%v, op=%v", op.SerialNum, sc.clientId2SerialNum[op.ClientId], op.String())
					sc.mu.Unlock()
					continue
				}
				// check lastApplied index
				if msg.CommandIndex <= sc.lastApplied {
					DebugLog(dJoin, sc, "outdated commit, op.SerialNum=%v, now=%v, op=%v", op.SerialNum, sc.clientId2SerialNum[op.ClientId], op.String())
					sc.mu.Unlock()
					continue
				}
				// update last applied and client's serial num
				sc.lastApplied = msg.CommandIndex
				sc.clientId2SerialNum[op.ClientId] = op.SerialNum

				//lastCfg := sc.configs[len(sc.configs)-1]
				switch op.OperationType {
				case JOIN:
					sc.doJoin(&op)
					break
				case LEAVE:
					sc.doLeave(&op)
					break
				case MOVE:
					sc.doMove(&op)
					break
				case QUERY:
					sc.doQuery(&op)
					break
				default:
					panic(fmt.Sprintf("unexpected op type=%v", op.OperationType))
				}
				opDoneChan := sc.getOpDoneChan(op.TransId, msg.CommandIndex)
				opDoneChan <- op // notifier the function who is waiting
				sc.mu.Unlock()

			}
		}
	}
}

func (sc *ShardCtrler) doJoin(op *Op) {
	lastCfg := sc.configs[len(sc.configs)-1]
	cfg := Join(lastCfg, JoinArgs{
		Servers:   deserializeServersJoined(op.SerializedServersJoined),
		ClientId:  op.ClientId,
		SerialNum: op.SerialNum,
		TransId:   op.TransId,
	})
	sc.configs = append(sc.configs, cfg)
	DebugLog(dJoin, sc, "[APPLIER] join action done, len Cfgs=%v, => ori=%v, now=%v, allcfgs=%v trans=%v", len(sc.configs),
		printGID2Shards(convertToNewG2S(lastCfg.Shards)), printGID2Shards(convertToNewG2S(cfg.Shards)), sc.printAllCfgs(), op.TransId)
}

func (sc *ShardCtrler) doLeave(op *Op) {
	lastCfg := sc.configs[len(sc.configs)-1]
	if len(sc.configs) == 1 {
		//op.ErrMsg = "can not remove GID since no config exists"
		//panic("can not remove GID since no config exists")
		DebugLog(dLeave, sc, "can not remove GID since no config exists")
	} else {
		cfg := Leave(lastCfg, LeaveArgs{GIDs: deserializeGidLeaved(op.SerializedGidLeaved), ClientId: op.ClientId, SerialNum: op.SerialNum})
		sc.configs = append(sc.configs, cfg)
		DebugLog(dLeave, sc, "[APPLIER] leave action done, len Cfgs=%v, => ori=%v, now=%v, allcfgs=%v trans=%v",
			len(sc.configs), printGID2Shards(convertToNewG2S(lastCfg.Shards)), printGID2Shards(convertToNewG2S(cfg.Shards)), sc.printAllCfgs(), op.TransId)
	}
}

func (sc *ShardCtrler) doMove(op *Op) {
	lastCfg := sc.configs[len(sc.configs)-1]
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
}

func (sc *ShardCtrler) doQuery(op *Op) {
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
	DebugLog(dQuery, sc, "[APPLIER] query action done, allcfgs=%v, trans=%v", sc.printAllCfgs(), op.TransId)
}
