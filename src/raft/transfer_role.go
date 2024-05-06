package raft

import "time"

const (
	FOLLOWER  Role = "follower"
	CANDIDATE      = "candidate"
	LEADER         = "leader"
)

// changeStateAndReinitialize
//
//	@Description: transit state between roles of follower/candidate/leader,
//	and reinitialize (e.g. reset/stop ticker, reallocate nextIndex and matchIndex)
//	@receiver rf
//	@param role: initial role {follower/candidate/leader}
func (rf *Raft) changeStateAndReinitialize(role Role) {
	if rf.state == role {
		return
	}
	DebugLog(dInfo, rf, "role transfer from %v to %v, stop heartbeat", string(rf.state), string(role))
	// leader -> {follower/candidate}
	if rf.state == LEADER {
		switch role {
		case FOLLOWER:
			rf.state = FOLLOWER
			rf.heartbeatTimeoutTicker.Stop()
			rf.resetElectionTicker()
			break
		case CANDIDATE:
			rf.state = CANDIDATE
			rf.heartbeatTimeoutTicker.Stop()
			rf.resetElectionTicker()
			break
		}
	} else { // follower/candidate -> {follower/candidate/leader}
		switch role {
		case LEADER: // to be leader, and send heartbeat immediately
			rf.state = LEADER
			// stop election ticker
			rf.electionTimeoutTicker.Stop()
			// reinitialized matchIndex[] and nextIndex[]
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.getLastLogIndex() + 1 // 122
				rf.matchIndex[i] = 0
			}
			// immediately trigger a heartbeat
			rf.heartbeatTimeoutTicker.Reset(time.Duration(rf.heartbeatTimeout) * time.Millisecond)
			go rf.sendAppendEntriesToAllPeers()
			// 3A: immediately commit a no-op log after being a leader
			if rf.commitNoop {
				go rf.Start(nil)
			}
			break
		case FOLLOWER:
			rf.state = FOLLOWER
			break
		case CANDIDATE:
			rf.state = CANDIDATE
			break
		}
	}
}
