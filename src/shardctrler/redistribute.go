package shardctrler

import "sort"

// getSortedKeys returns the sorted keys of a map where the keys are integers and values can be of any type.
func getSortedKeys[V any](mapping map[int]V) []int {
	var keys []int
	for key := range mapping {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	return keys
}

// FirstJoin conducts the join when previous Config does not exist.
//
//	@Description:
//	@param joinArgs: Servers map[int][]string // new GID -> servers mappings
func FirstJoin(joinArgs JoinArgs) (firstCfg Config) {
	nGroups := len(joinArgs.Servers)
	shardsPerGroup := NShards / nGroups
	remainder := NShards % nGroups
	shardIdx := 0
	// allocate shards to each group
	gid2shards := make(map[int][]int)
	gids := make([]int, 0, nGroups)
	// allocate main shards evenly
	for _, gid := range getSortedKeys(joinArgs.Servers) {
		for j := 0; j < shardsPerGroup; j++ {
			gid2shards[gid] = append(gid2shards[gid], shardIdx)
			shardIdx++
		}
		gids = append(gids, gid)
	}
	// allocate remainder shards to group
	for i := 0; i < remainder; i++ {
		gid2shards[gids[i]] = append(gid2shards[gids[i]], shardIdx)
		shardIdx++
	}
	// reverse gid->shards to be shards->gid
	shard2gid := convertToS2G(gid2shards)
	DebugLog(dJoin, "first join res=%v", printGID2Shards(gid2shards))
	firstCfg.Num = 0
	firstCfg.Groups = joinArgs.Servers
	firstCfg.Shards = shard2gid
	return
}

func Join(currCfg Config, joinArgs JoinArgs) (newCfg Config) {

	// get current number of groups
	nNewGroups := len(joinArgs.Servers)
	nGroups := len(currCfg.Groups) + nNewGroups
	// new GIDS slice
	newGIDs := make([]int, 0, nNewGroups)
	for _, newGID := range getSortedKeys(joinArgs.Servers) {
		newGIDs = append(newGIDs, newGID)
	}
	// distribute shards evenly
	shardsPerGroup := NShards / nGroups
	remainder := NShards % nGroups
	// get the shard Ids that should be
	shardsForNewGroup := make([]int, 0, (shardsPerGroup+1)*nNewGroups)
	// variables for new Config
	newGid2Sids := make(map[int][]int, nGroups)
	// remove shards from existing groups
	oldGid2Sids := convertToG2S(currCfg.Shards)
	for _, gid := range getSortedKeys(oldGid2Sids) {
		shards := oldGid2Sids[gid]
		boundary := shardsPerGroup + imin(1, remainder)
		reducedShards := shards[:boundary]
		removedShards := shards[boundary:]
		if remainder > 0 {
			remainder--
		}
		// append the shards that will be allocated to new GID
		shardsForNewGroup = append(shardsForNewGroup, removedShards...)
		newGid2Sids[gid] = reducedShards
	}
	// Optional, sort the shards for the new GID
	sort.Slice(shardsForNewGroup, func(i, j int) bool {
		return shardsForNewGroup[i] < shardsForNewGroup[j]
	})
	// use robin-round to allocate shards for new GIDS
	for idx, sid := range shardsForNewGroup {
		gid := newGIDs[idx%nNewGroups]
		newGid2Sids[gid] = append(newGid2Sids[gid], sid)
	}
	// reverse key/value
	shard2gid := convertToS2G(newGid2Sids)
	DebugLog(dJoin, "originalCfg=%v, updatedCfg=%v",
		printGID2Shards(convertToG2S(currCfg.Shards)), printGID2Shards(newGid2Sids))
	newGroups := make(map[int][]string, nGroups)
	// Copying each slice from the original map to the new map
	for key, slice := range currCfg.Groups {
		sliceCopied := make([]string, len(slice))
		copy(sliceCopied, slice)
		newGroups[key] = sliceCopied
	}
	// Add new GID -> server mappings
	for newGID, servers := range joinArgs.Servers {
		newGroups[newGID] = servers
	}
	newCfg.Num = currCfg.Num + 1
	newCfg.Shards = shard2gid
	newCfg.Groups = newGroups
	return
}

func convertToG2S(shard2gid [NShards]int) map[int][]int {
	gid2shards := make(map[int][]int)
	for sid, gid := range shard2gid {
		gid2shards[gid] = append(gid2shards[gid], sid)
	}
	return gid2shards
}

func convertToS2G(gid2shards map[int][]int) [NShards]int {
	var shard2gid [NShards]int
	for gid, shards := range gid2shards {
		for _, sid := range shards {
			shard2gid[sid] = gid
		}
	}
	return shard2gid
}

func imax(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func imin(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
