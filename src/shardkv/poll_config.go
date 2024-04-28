package shardkv

// updateConfig sends RPC to shardctrler for the latest Config
// Once it gets the reply, it updates the kv server's config
func (kv *ShardKV) updateConfig() {
	cfg := kv.scc.Query(-1)
	kv.cfgMutex.Lock()
	defer kv.cfgMutex.Unlock()
	if cfg.Num == kv.latestCfg.Num {
		kv.latestCfg = cfg
	} else if cfg.Num > kv.latestCfg.Num {
		prevCfg := kv.latestCfg
		kv.latestCfg = cfg
		if cfg.Num == 1 {
			// do nothing
		} else {
			// TODO do extra action since now the Config changed
			if kv.latestCfg.Num == prevCfg.Num+1 {
				// case 1: preNum = 8, nowNum = 9, fetch/send extra/removed shard to other groups
			} else {
				// case 2: preNum = 2, nowNum = 999, which means the server downs from 6, and server down in that range TODO: do what?
			}

		}

	}
	//else {
	//	panic(fmt.Sprintf("warning: updateConfig() try to update an older Config, fetchedCfg=%v, curr=%v", cfg, kv.latestCfg))
	//}
}

// ticker handles tick emission and do correspondent actions
func (kv *ShardKV) ticker() {
	for !kv.killed() {
		select {
		case <-kv.cfgPollingTicker.C:
			go kv.updateConfig()
		}
	}
}
