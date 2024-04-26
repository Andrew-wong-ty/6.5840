package shardctrler

import (
	"fmt"
	"log"
	rand2 "math/rand"
	"testing"
)

// check if the shards are distributed evenly among GIDs
func isDistributedEvenly(shards [NShards]int) bool {
	MAX := 0
	MIN := 9999
	var gid2sid map[int][]int
	gid2sid = convertToNewG2S(shards)
	for _, shards := range gid2sid {
		MAX = imax(MAX, len(shards))
		MIN = imin(MIN, len(shards))
	}
	if MAX-MIN > 1 {
		return false
	} else {
		return true
	}
}

func generateServerSlice() []string {
	res := make([]string, 0)
	rand := rand2.Int()%5 + 1
	for i := 0; i < rand; i++ {
		res = append(res, fmt.Sprintf("%v-%v", rand2.Int()%100, i))
	}
	return res
}

func generateJoinArg(nNewGroup, GIDStartId int) (args JoinArgs) {
	gid2servers := make(map[int][]string)
	for gid := GIDStartId; gid < GIDStartId+nNewGroup; gid++ {
		gid2servers[gid] = generateServerSlice()
	}
	args.Servers = gid2servers
	return
}
func TestManyFirstJoin(t *testing.T) {
	for i := 0; i < 50; i++ { // test 50 times
		for gCount := 1; gCount <= 10; gCount++ {
			joinArgs := generateJoinArg(gCount, 0)
			firstCfg := FirstJoin(joinArgs)
			if !isDistributedEvenly(firstCfg.Shards) {
				log.Fatalln("shards did not distribute evenly among GIDs")
			}
		}
	}
}

func TestMove(t *testing.T) {
	for gCount := 2; gCount <= 10; gCount++ {
		joinArgs := generateJoinArg(gCount, 0)
		firstCfg := FirstJoin(joinArgs)
		for i := 0; i < NShards; i++ {
			for j := 0; j < gCount; j++ {
				cfg := Move(firstCfg, MoveArgs{
					Shard: i,
					GID:   j,
				})
				if !isDistributedEvenly(cfg.Shards) {
					log.Fatalln("shards did not distribute evenly among GIDs")
				}
			}
		}
	}
}

func secondJoin(firstNGroup, secondNNewGroup int) Config {
	joinArgs := generateJoinArg(firstNGroup, 0)
	firstCfg := FirstJoin(joinArgs)
	joinArgs2 := generateJoinArg(secondNNewGroup, firstNGroup)
	newCfg := Join(firstCfg, joinArgs2)
	if !isDistributedEvenly(newCfg.Shards) {
		log.Fatalln("shards did not distribute evenly among GIDs")
	}
	return newCfg
}

func TestSecondJoin(t *testing.T) {
	for i := 1; i <= 9; i++ {
		for j := 1; j <= NShards-i; j++ {
			DebugLog(dJoin, nil, "1st = %v, newCnt= %v\n", i, j)
			secondJoin(i, j)
		}
	}
}

func TestManyJoin(t *testing.T) {
	for i := 1; i <= 3; i++ {
		for j := 1; j <= 3; j++ {
			for k := 1; k <= 4; k++ {
				DebugLog(dJoin, nil, "%v <- %v = %v, %v <- %v\n", i, j, i+j, i+j, k)
				// j join i
				cfg2 := secondJoin(i, j)
				joinArgs3 := generateJoinArg(k, i+j)
				cfg3 := Join(cfg2, joinArgs3)
				if !isDistributedEvenly(cfg3.Shards) {
					log.Fatalln("shards did not distribute evenly among GIDs")
				}
			}
		}
	}
}

func TestSingleLeave(t *testing.T) {
	cfg1 := FirstJoin(generateJoinArg(10, 0))
	cfg2 := Leave(cfg1, LeaveArgs{GIDs: []int{3}})
	if !isDistributedEvenly(cfg2.Shards) {
		log.Fatalln("shards did not distribute evenly among GIDs")
	}
}

func TestJoinThenLeave(t *testing.T) {
	for i := 1; i < NShards; i++ {
		for j := 1; j <= NShards-i; j++ {

			joinArgs := generateJoinArg(i, 0)
			cfg1 := FirstJoin(joinArgs)
			joinArgs2 := generateJoinArg(j, i)
			cfg2 := Join(cfg1, joinArgs2)
			leaveGIDs := make([]int, 0)
			for k := 0; k < i+j; k++ {
				leaveGIDs = append(leaveGIDs, k)
			}
			for k := 1; k < i+j; k++ {
				rand2.Shuffle(len(leaveGIDs), func(i, j int) {
					leaveGIDs[i], leaveGIDs[j] = leaveGIDs[j], leaveGIDs[i]
				})
				DebugLog(dJoin, nil, "=>  1st_join = %v, 2st_join = %v, leave=%v\n", i, j, leaveGIDs[:k])
				cfg3 := Leave(cfg2, LeaveArgs{GIDs: leaveGIDs[:k]})
				if !isDistributedEvenly(cfg3.Shards) {
					log.Fatalln("shards did not distribute evenly among GIDs")
				}
			}

		}
	}
}

func TestExtraJoinAndLeave(t *testing.T) {
	joinArgs := generateJoinArg(8, 0)
	cfg1 := FirstJoin(joinArgs)
	if len(cfg1.Groups) != 8 {
		panic("unmatch results")
	}
	joinArgs2 := generateJoinArg(6, 8) // 4 extra group
	cfg2 := Join(cfg1, joinArgs2)
	if len(cfg2.Groups) != 14 {
		panic("unmatch results")
	}
	// test leave GIDs contain GIDs with signed or unsigned shards
	{
		cfg3 := Leave(cfg2, LeaveArgs{GIDs: []int{6, 7, 8, 9, 10, 11, 12}})
		gid2shards := convertToNewG2S(cfg3.Shards)
		if len(gid2shards) != 7 || len(cfg3.Groups) != 7 {
			panic("unmatch results")
		}
		for _, gid := range []int{0, 1, 2, 3, 4, 5, 13} {
			if _, exist := gid2shards[gid]; !exist {
				panic("unmatch results")
			}
			if _, exist := cfg3.Groups[gid]; !exist {
				panic("unmatch results")
			}
		}
	}
	// test leave GIDs contain GIDs only with signed shards
	{
		cfg3 := Leave(cfg2, LeaveArgs{GIDs: []int{8, 9}})
		gid2shards := convertToNewG2S(cfg3.Shards)
		if len(gid2shards) != 10 || len(cfg3.Groups) != 12 {
			panic("unmatch results")
		}
		for _, gid := range []int{0, 1, 2, 3, 4, 5, 6, 7, 10, 11} {
			if _, exist := gid2shards[gid]; !exist {
				panic("unmatch results")
			}
			if _, exist := cfg3.Groups[gid]; !exist {
				panic("unmatch results")
			}
		}
	}
	// test leave GIDs contain GIDs only with signed shards, and GIDs with unsigned shards just fills the leave ones
	{
		cfg3 := Leave(cfg2, LeaveArgs{GIDs: []int{6, 7, 8, 9}})
		gid2shards := convertToNewG2S(cfg3.Shards)
		if len(gid2shards) != 10 || len(cfg3.Groups) != 10 {
			panic("unmatch results")
		}
		for _, gid := range []int{0, 1, 2, 3, 4, 5, 10, 11, 12, 13} {
			if _, exist := gid2shards[gid]; !exist {
				panic("unmatch results")
			}
			if _, exist := cfg3.Groups[gid]; !exist {
				panic("unmatch results")
			}
		}
	}
	// test leave GIDs contain GIDs only without signed shards
	{
		cfg3 := Leave(cfg2, LeaveArgs{GIDs: []int{10, 11, 12, 13}})
		gid2shards := convertToNewG2S(cfg3.Shards)
		if len(gid2shards) != 10 || len(cfg3.Groups) != 10 {
			panic("unmatch results")
		}
		for _, gid := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} {
			if _, exist := gid2shards[gid]; !exist {
				panic("unmatch results")
			}
			if _, exist := cfg3.Groups[gid]; !exist {
				panic("unmatch results")
			}
		}
	}
}
