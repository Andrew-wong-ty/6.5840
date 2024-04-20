package shardctrler

import (
	"fmt"
	"log"
	"sort"
	"time"
)

// code reference: https://blog.josejg.com/debugging-pretty/

type logTopic string

const (
	dJoin logTopic = "JOIN"
)

var debugStart time.Time
var debugVerbosity int

func getVerbosity() int {
	return 1
	//v := os.Getenv("VERBOSE")
	//level := 0
	//if v != "" {
	//	var err error
	//	level, err = strconv.Atoi(v)
	//	if err != nil {
	//		log.Fatalf("Invalid verbosity %v", v)
	//	}
	//}
	//return level
}

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DebugLog(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		currTime := time.Since(debugStart).Microseconds()
		currTime /= 100
		prefix := fmt.Sprintf("%06d %v ", currTime, string(topic))

		format = prefix + format
		log.Printf(format, a...)
	}
}

func printGID2Shards(gid2shards map[int][]int) string {
	res := ""
	for _, gid := range getSortedKeys(gid2shards) {
		shards := gid2shards[gid]
		sort.Ints(shards)
		res += fmt.Sprintf("{%v->%v} ", gid, shards)
	}
	return res
}
