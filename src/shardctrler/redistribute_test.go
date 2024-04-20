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
	gid2sid = convertToG2S(shards)
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
func TestMultipleFirstJoin(t *testing.T) {
	for i := 0; i < 50; i++ { // test 50 times
		for gCount := 1; gCount <= 10; gCount++ {
			joinArgs := generateJoinArg(gCount, 0)
			firstCfg := FirstJoin(joinArgs)
			if firstCfg.Num != 0 {
				log.Fatalln("unexpected sequence number")
			}
			if !isDistributedEvenly(firstCfg.Shards) {
				log.Fatalln("shards did not distribute evenly among GIDs")
			}
		}
	}
}

func secondJoin(firstNGroup, secondNNewGroup int) Config {
	joinArgs := generateJoinArg(firstNGroup, 0)
	firstCfg := FirstJoin(joinArgs)
	joinArgs2 := generateJoinArg(secondNNewGroup, firstNGroup)
	newCfg := Join(firstCfg, joinArgs2)
	if newCfg.Num != 1 {
		log.Fatalln("unexpected sequence number")
	}
	if !isDistributedEvenly(newCfg.Shards) {
		log.Fatalln("shards did not distribute evenly among GIDs")
	}
	return newCfg
}

func TestSecondJoin(t *testing.T) {
	for i := 1; i <= 9; i++ {
		for j := 1; j <= NShards-i; j++ {
			DebugLog(dJoin, "1st = %v, newCnt= %v\n", i, j)
			secondJoin(i, j)
		}
	}
}

func TestMultipleJoin(t *testing.T) {
	for i := 1; i <= 3; i++ {
		for j := 1; j <= 3; j++ {
			for k := 1; k <= 4; k++ {
				DebugLog(dJoin, "%v <- %v = %v, %v <- %v\n", i, j, i+j, i+j, k)
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
