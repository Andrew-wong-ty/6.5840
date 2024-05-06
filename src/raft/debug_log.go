package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// code reference: https://blog.josejg.com/debugging-pretty/

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

func getVerbosity() int {
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

func DebugLog(topic logTopic, rf *Raft, format string, a ...interface{}) {
	if debugVerbosity >= 3 {
		currTime := time.Since(debugStart).Microseconds()
		currTime /= 100
		prefix := fmt.Sprintf("%06d %v ", currTime, string(topic))
		S := fmt.Sprintf("S%d ", rf.me)
		T := fmt.Sprintf("T%d ", rf.currentTerm)
		format = prefix + S + T + format
		log.Printf(format, a...)
	}
}

// logs2str
//
//	@Description: convert rf.logs to string. for debug
//	@receiver rf
//	@param logs
//	@return string
func (rf *Raft) logs2str(logs []Log) string {
	res := ""
	l := len(logs)
	for idx, logItem := range logs {
		//res += fmt.Sprintf("{idx=%v t=%v cmd=%v}, ", rf.physicalIdx2logIdx(idx), logItem.Term, convertCommandToString(logItem.Command))
		if idx == 0 {
			res += fmt.Sprintf("{idx=%v t=%v cmd=%v}, ...", idx, logItem.Term, convertCommandToString(logItem.Command))
		}
		if idx >= l-1 {
			res += fmt.Sprintf("{idx=%v t=%v cmd=%v}, ", idx, logItem.Term, convertCommandToString(logItem.Command))

		}
	}
	return "[" + res + "]"
}

func (rf *Raft) PrintAllIndicesAndTermsStates() string {
	vf := -1
	if rf.votedFor != nil {
		vf = *rf.votedFor
	}
	res := fmt.Sprintf("T=%v, 1stLogIdx=%v, lastLogIdx=%v, logLen=%v, comittedIdx=%v, lastAppliedIdx=%v, SnapLastIncludeIdx=%v, SnapLastIncludeTerm=%v, role=%v, vf=%v",
		rf.currentTerm, rf.firstLogIdx, rf.getLastLogIndex(), len(rf.logs), rf.commitIndex, rf.lastApplied, rf.snapshotLastIncludedIdx, rf.snapshotLastIncludedTerm, rf.state, vf)
	return res
}
