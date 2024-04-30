package shardctrler

import (
	"fmt"
	"sort"
)

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
	if len(gid2shards) != len(joinArgs.Servers) {
		panic("unbalanced groups")
	}
	shard2gid := convertToNewS2G(gid2shards)
	//DebugLog(dJoin, "first join res=%v", printGID2Shards(gid2shards))
	firstCfg.Num = 1
	firstCfg.Groups = joinArgs.Servers
	firstCfg.Shards = shard2gid
	return
}

// Some GIDs that are leaving may not have been assigned shards.
// In this case, remove them from both currCfg.Groups and leaveArgs.GIDs
func updateByRemoveGIDsWithoutSignedShards(currCfg Config, leaveArgs LeaveArgs) (updatedCfg Config, updatedLeaveArgs LeaveArgs) {
	updatedCfg = DeepCopyConfig(currCfg)
	updatedLeaveArgs = leaveArgs // shallow copy
	gid2shards := convertToNewG2S(currCfg.Shards)
	newLeaveGIDs := make([]int, 0)
	for _, gid := range leaveArgs.GIDs {
		if _, exist := gid2shards[gid]; !exist {
			// a group without assigned shards is leaving -> delete from group
			delete(updatedCfg.Groups, gid)
		} else {
			newLeaveGIDs = append(newLeaveGIDs, gid)
		}
	}
	// assign new GIDs, where GIDs without signed shards are removed
	updatedLeaveArgs.GIDs = newLeaveGIDs
	return
}

func checkLeaveArgsValid(currCfg Config, leaveArgs LeaveArgs) {
	// no duplicate elements in slice
	if isSliceHasDuplicateElements(leaveArgs.GIDs) {
		panic("remove the same GID multiple times in a single operation")
	}
	// the gids must be presented in groups
	for _, gid := range leaveArgs.GIDs {
		if _, exist := currCfg.Groups[gid]; !exist {
			panic(fmt.Sprintf("the gid %v to be removed not presents in cfg.groups; currCfg:%v, leaveArg:%v",
				gid, printCfg(currCfg), leaveArgs))
		}
	}
}

// old group, gids in leaveGIDs are going to leave the old group
// returns the sorted new group ID
func getNewGIDs(groups map[int][]string, leaveGIDs []int) []int {
	res := make([]int, 0)
	groupsCpy := deepCopyMap(groups)
	for _, gid := range leaveGIDs {
		delete(groupsCpy, gid)
	}
	for _, gid := range getSortedKeys(groupsCpy) {
		res = append(res, gid)
	}
	return res
}
func Leave(currCfg Config, leaveArgs LeaveArgs) (newCfg Config) {
	dbgStr := fmt.Sprintf("[LEAVE] called, curr=%v, leaveArgs=%v", printCfg(currCfg), leaveArgs)
	checkLeaveArgsValid(currCfg, leaveArgs)
	currCfg, leaveArgs = updateByRemoveGIDsWithoutSignedShards(currCfg, leaveArgs)
	// if no GIDs are leaving, return a copied one with incremented Num
	if len(leaveArgs.GIDs) == 0 {
		newCfg = DeepCopyConfig(currCfg)
		newCfg.Num = currCfg.Num + 1
		return
	}
	removedShards := make([]int, 0, NShards)
	newGID2Shards := convertToNewG2S(currCfg.Shards)
	newGroups := deepCopyMap(currCfg.Groups)
	// get all removed shards, and delete leaving GIDs
	for _, gid := range leaveArgs.GIDs {
		removedShards = append(removedShards, newGID2Shards[gid]...)
		delete(newGID2Shards, gid)
		delete(newGroups, gid)
	}
	sort.Ints(removedShards)
	nGroups := len(currCfg.Groups) - len(leaveArgs.GIDs)
	newGroupIDs := getNewGIDs(currCfg.Groups, leaveArgs.GIDs) // the new GIDs after some GIDs left
	if nGroups != len(newGroups) {
		panic("bug")
	}
	if nGroups == 0 {
		newCfg.Num = currCfg.Num + 1
		return
	}
	shardsPerGroup := NShards / nGroups
	remainder := NShards % nGroups
	for _, gid := range newGroupIDs {
		shards := newGID2Shards[gid]
		// append shards to it
		need := shardsPerGroup - len(shards) + min(1, remainder)
		if need > 0 {
			shards = append(shards, removedShards[:need]...)
			removedShards = removedShards[need:]
			if remainder > 0 {
				remainder--
			}
		}
		newGID2Shards[gid] = shards
		if len(removedShards) == 0 {
			break
		}
	}
	if len(removedShards) != 0 {
		panic(fmt.Sprintf("removedShards should all be distributed, currCfg=%v, leaveArgs=%v\n, args=%v",
			printGID2Shards(convertToNewG2S(currCfg.Shards)), leaveArgs.GIDs, currCfg))
	}
	//DebugLog(dJoin, "originalCfg=%v, updatedCfg=%v",
	//	printGID2Shards(convertToNewG2S(currCfg.Shards)), printGID2Shards(newGID2Shards))
	checkAllShardsAllocated(newGID2Shards)
	newCfg.Num = currCfg.Num + 1
	newCfg.Shards = convertToNewS2G(newGID2Shards)
	newCfg.Groups = newGroups
	checkCfgValid(newCfg)
	DebugLog(dLeave, nil, fmt.Sprintf("%v, res=%v\n", dbgStr, printCfg(newCfg)))
	return
}

// Move a shard to a GID based on the current Configuration;
// Config.Num should increment
func Move(currCfg Config, moveArgs MoveArgs) (newCfg Config) {
	if _, exist := currCfg.Groups[moveArgs.GID]; !exist {
		panic(fmt.Sprintf("target GID %v non-exist", moveArgs.GID))
	}
	//DebugLog(dMove, nil, "ori=%v, move sid=%v to gid=%v", printGID2Shards(convertToNewG2S(currCfg.Shards)), moveArgs.Shard, moveArgs.GID)
	if currCfg.Shards[moveArgs.Shard] == moveArgs.GID {
		// it is going to move shard to its current group, create a new cfg then return
		newCfg.Num = currCfg.Num + 1
		newCfg.Groups = deepCopyMap(currCfg.Groups)
		newCfg.Shards = deepCopyArray(currCfg.Shards)
		DebugLog(dMove, nil, "[MOVE] called, curr=%v, moveArgs=%v, res=%v\n", printCfg(currCfg), moveArgs, printCfg(newCfg))
		checkCfgValid(newCfg)
		return
	} else {
		newShards := deepCopyArray(currCfg.Shards)
		sidTobeSwapped := -1
		for i := NShards - 1; i >= 0; i-- {
			if newShards[i] == moveArgs.GID {
				sidTobeSwapped = i
				break
			}
		}
		if sidTobeSwapped == -1 {
			panic("unexpected sid " + fmt.Sprintf("currcfg=%v, moveArgs=%v", currCfg, moveArgs))
		}
		newShards[sidTobeSwapped] = newShards[moveArgs.Shard]
		newShards[moveArgs.Shard] = moveArgs.GID
		//DebugLog(dJoin, "originalCfg=%v, movedCfg=%v",
		//	printGID2Shards(convertToNewG2S(currCfg.Shards)), printGID2Shards(convertToNewG2S(newShards)))
		newCfg.Num = currCfg.Num + 1
		newCfg.Groups = deepCopyMap(currCfg.Groups)
		newCfg.Shards = newShards
		DebugLog(dMove, nil, "[MOVE] called, curr=%v, moveArgs=%v, res=%v\n", printCfg(currCfg), moveArgs, printCfg(newCfg))
		if len(currCfg.Groups) != len(newCfg.Groups) {
			panic("move; unbalanced groups")
		}
		checkCfgValid(newCfg)
		return
	}
}

func Join(currCfg Config, joinArgs JoinArgs) (newCfg Config) {
	dbgMsg := fmt.Sprintf("[JOIN] called, curr=%v, joinArgs=%v", printCfg(currCfg), joinArgs)
	if isCfgObjectZero(currCfg) {
		cfg := FirstJoin(joinArgs)
		newCfg.Num = currCfg.Num + 1
		newCfg.Shards = cfg.Shards
		newCfg.Groups = cfg.Groups
		return
	}
	oldGid2Sids := convertToNewG2S(currCfg.Shards)
	if len(oldGid2Sids) == NShards { // group full, do not allocate any shards to extra groups
		newCfg = newCfgWhenGroupFull(currCfg, joinArgs)
		return
	}
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
	for _, gid := range getSortedKeys(oldGid2Sids) {
		shards := oldGid2Sids[gid]
		boundary := shardsPerGroup + imin(1, remainder)
		if boundary <= len(shards) { // the shard has somthing to be removed
			reducedShards := shards[:boundary]
			removedShards := shards[boundary:]
			if remainder > 0 {
				remainder--
			}
			// append the shards that will be allocated to new GID
			shardsForNewGroup = append(shardsForNewGroup, removedShards...)
			newGid2Sids[gid] = reducedShards
		} else {
			newGid2Sids[gid] = shards
		}
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
	shard2gid := convertToNewS2G(newGid2Sids)
	checkAllShardsAllocated(newGid2Sids)
	newGroups := deepCopyMap(currCfg.Groups)
	// Add new GID -> server mappings
	for newGID, servers := range joinArgs.Servers {
		newGroups[newGID] = servers
	}
	newCfg.Num = currCfg.Num + 1
	newCfg.Shards = shard2gid
	newCfg.Groups = newGroups
	DebugLog(dJoin, nil, dbgMsg+fmt.Sprintf(" res=%v", printCfg(newCfg)))
	checkCfgValid(newCfg)
	return
}

func checkAllShardsAllocated(newGid2Sids map[int][]int) {
	shardsMap := make(map[int]bool)
	for _, shards := range newGid2Sids {
		for _, shard := range shards {
			shardsMap[shard] = true
		}
	}
	if len(shardsMap) != NShards {
		panic("some shards are not allocated")
	}
}

func convertToNewG2S(shard2gid [NShards]int) map[int][]int {
	gid2shards := make(map[int][]int)
	for sid, gid := range shard2gid {
		gid2shards[gid] = append(gid2shards[gid], sid)
	}
	return gid2shards
}

func convertToNewS2G(gid2shards map[int][]int) [NShards]int {
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

// deepCopyMap takes a map[int][]string and returns a deep copy of it.
func deepCopyMap(originalMap map[int][]string) map[int][]string {
	// Create a new map with the same capacity as the original map.
	copiedMap := make(map[int][]string, len(originalMap))

	// Iterate over the original map.
	for key, valueSlice := range originalMap {
		// Make a new slice to hold the copy of the strings.
		sliceCopy := make([]string, len(valueSlice))
		copy(sliceCopy, valueSlice)

		// Assign the copied slice to the new map under the same key.
		copiedMap[key] = sliceCopy
	}

	return copiedMap
}

// isSliceHasDuplicateElements checks if there are any duplicate integers in the slice.
func isSliceHasDuplicateElements(slice []int) bool {
	seen := make(map[int]bool) // map to keep track of seen integers

	for _, value := range slice {
		if _, exists := seen[value]; exists {
			return true // If the value exists already, return true
		}
		seen[value] = true // Mark this value as seen
	}

	return false // If no duplicates were found, return false
}

func deepCopyArray(source [NShards]int) [NShards]int {
	var copyInts [NShards]int
	for i, value := range source {
		copyInts[i] = value
	}
	return copyInts
}

// check if a config is a zero value
func isCfgObjectZero(c Config) bool {
	// Check if Groups is nil (not just empty)
	if c.Groups != nil {
		return false
	}
	// Check if all elements in Shards are zero
	for _, shard := range c.Shards {
		if shard != 0 {
			return false
		}
	}
	return true
}

func IsConfigZero(c Config) bool {
	// Check if all elements in Shards are zero
	for _, shard := range c.Shards {
		if shard != 0 {
			return false
		}
	}
	// Check if Groups is nil (not just empty)
	if c.Num == 0 && (c.Groups == nil || len(c.Groups) == 0) {
		return true
	}
	return false
}

// checkCfgValid determines if a config is valid by checking
// 1. if shards.groups.len <10, cfg.groups and groups in shard must match each other
// 2. else, the groups in shards must present in cfg.groups
func checkCfgValid(cfg Config) {
	gid2shards := convertToNewG2S(cfg.Shards)
	// gids should be presented in cfg.groups
	for gid, _ := range gid2shards {
		if _, exist := cfg.Groups[gid]; !exist {
			panic(fmt.Sprintf("gid %v is not presented in cfg.Groups (%v); cfg=%v", gid, cfg.Groups, cfg.String()))
		}
	}
	if len(gid2shards) < 10 {
		// the gids in groups should be presented in gid2shards
		for gid, _ := range cfg.Groups {
			if _, exist := gid2shards[gid]; !exist {
				panic(fmt.Sprintf("gid %v is not presented in gid2shards (%v)", gid, gid2shards))
			}
		}
	}
}

// print a Config object
// TODO: make it to be Config's method
func printCfg(cfg Config) string {
	gid2shards := printGID2Shards(convertToNewG2S(cfg.Shards))
	groups := cfg.Groups
	return fmt.Sprintf("{num= %v gid2shards=%v, groups=%v, lenG=%v}", cfg.Num, gid2shards, groups, len(groups))
}

func DeepCopyConfig(currCfg Config) (newCfg Config) {
	newCfg.Num = currCfg.Num
	newCfg.Shards = deepCopyArray(currCfg.Shards)
	newCfg.Groups = deepCopyMap(currCfg.Groups)
	return
}

// newCfgWhenGroupFull each shard has been allocated to a distinct group. So the currently joining groups should
// not affect the shard <-> groups relations. Therefore, return a new config with updated groups.
func newCfgWhenGroupFull(currCfg Config, joinArgs JoinArgs) (newCfg Config) {
	newCfg = DeepCopyConfig(currCfg)
	newCfg.Num = currCfg.Num + 1
	for gid, servers := range joinArgs.Servers {
		newCfg.Groups[gid] = servers
	}
	return newCfg
}
