package raft

import (
	"math/rand"
	"time"
)

// ticker is goroutine that receives msg from the election ticker and appendEntries ticker,
// and do correspondent actions
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimeoutTicker.C:
			go rf.electMyselfToBeLeader()
		case <-rf.heartbeatTimeoutTicker.C:
			go rf.sendAppendEntriesToAllPeers()
		}
	}
}

// resetElectionTicker when
//  1. receives HB from leader
//  2. role transferred from leader to {follower/candidate}
//  3. this machine granted vote to a leader
func (rf *Raft) resetElectionTicker() time.Duration {
	ms := rf.electionTimeout + (rand.Int63() % 50) //ok 500+-50
	duration := time.Duration(ms) * time.Millisecond
	rf.electionTimeoutTicker.Reset(duration)
	return duration
}
