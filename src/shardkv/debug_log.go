package shardkv

import (
	"6.5840/shardctrler"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// code reference: https://blog.josejg.com/debugging-pretty/

type logTopic string

const (
	dJoin         logTopic = "JOIN"
	dLeave                 = "LEAVE"
	dMove                  = "MOVE"
	dQuery                 = "QUERY"
	dApply                 = "APPLY"
	dSnap                  = "SNAP"
	dPollCfg               = "POLL"
	dGet                   = "GET"
	dPut                   = "PUT"
	dAppend                = "APPEND"
	dClient                = "CLNT"
	dCheck                 = "CHECK"
	dSend                  = "SEND"
	dReceive               = "RECEIVE"
	dMigrate               = "MIGRATE"
	dMark                  = "MARK_NOT_READY"
	dInstallShard          = "INSTALL_SHARD"
)

var debugStart time.Time
var debugVerbosity int

func getVerbosity() int {
	//return 1
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DebugLog(topic logTopic, kv *ShardKV, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		currTime := time.Since(debugStart).Microseconds()
		currTime /= 100
		prefix := fmt.Sprintf("%06d %v ", currTime, string(topic))
		if kv != nil {
			prefix = prefix + fmt.Sprintf("S%v ", kv.me) + fmt.Sprintf("GID:%v ", kv.gid) + fmt.Sprintf("[%v] ", topic)
		}
		format = prefix + format
		log.Printf(format, a...)
	}
}

func db2str(db [shardctrler.NShards]Shard) string {
	res := " "
	for shardId, subDB := range db {
		res += fmt.Sprintf("%vv%v_l%v; ", shardId, subDB.Version, len(subDB.Data))
	}
	return res
}
