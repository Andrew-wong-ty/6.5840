package raft

import "fmt"

/****************************************************  AppendEntries  *******************************************************/

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int
	EventId      string // *only for debug* : An Id represents the event of leader sending AppendEntries to all peers
}

func (aa AppendEntriesArgs) toString() string {
	res := fmt.Sprintf("[Term:%v LeaderId:%v PrevLogIndex:%v PrevLogTerm:%v, LeaderCommit:%v, EventId:%v, ]", aa.Term, aa.LeaderId, aa.PrevLogIndex, aa.PrevLogTerm, aa.LeaderCommit, aa.EventId)
	return res
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	/*  optional
	when rejecting an AppendEntries request, the follower
	can include the term of the conflicting entry and
	the first index it stores for that term.
	*/
	// 'X' means 'conflict'
	XTerm  int //term in the conflicting entry (if any)
	XIndex int //index of first entry with that term (if any)
	XLen   int //log length
}

// AppendEntries RPC handler (for receivers)
/*** handler ***/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	defer rf.persist() //persist before the server replies to an ongoing RPC.
	defer rf.resetElectionTicker()
	// Rule.All Servers.1
	defer func() {
		if rf.commitIndex > rf.lastApplied {
			DebugLog(dInfo, rf, "notify myself apply logs, STATES: %v", rf.PrintAllIndicesAndTermsStates())
			rf.applyCond.Signal() // notify apply logs
		}
	}()
	// fill XMessage when fail
	defer func() {
		reply.XLen, reply.XIndex, reply.XTerm = -1, -1, -1
		if reply.Success == false {
			reply.XLen = rf.physicalIdx2logIdx(len(rf.logs)) // Xlen = logs.lastLogIndex+1
			if rf.logIdx2physicalIdx(args.PrevLogIndex) <= 0 {
				return // It does not have entrie with index of "args.PrevLogIndex". This happens when logs are in snapshot and they are discarded
			} else if rf.getLastLogIndex() >= args.PrevLogIndex {
				if rf.logs[rf.logIdx2physicalIdx(args.PrevLogIndex)].Term != args.PrevLogTerm {
					reply.XTerm = rf.logs[rf.logIdx2physicalIdx(args.PrevLogIndex)].Term
					// find the first index with XTerm
					reply.XIndex = -1
					for idx, logItem := range rf.logs {
						if logItem.Term == reply.XTerm {
							reply.XIndex = rf.physicalIdx2logIdx(idx)
							break
						}
					}
					if reply.XIndex == -1 {
						panic("unexpected: reply.XIndex == -1")
					}
					DebugLog(dLog, rf, "eid=%v, follower: has index=%v but term doesn't match term, his=%v myXterm=%v", args.EventId, args.PrevLogIndex, args.PrevLogTerm, reply.XTerm)
				}
			} else {
				// too short, it doesn't have index=args.PrevLogIndex
				DebugLog(dLog, rf, "eid=%v, follower: I am too short", args.EventId)
			}
		}
	}()

	reply.Term = max(rf.currentTerm, args.Term)
	reply.Success = true
	// Rules for Servers->All Servers->2: RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		DebugLog(dInfo, rf, "convert rf.votedFor to nil ")
		rf.votedFor = nil
		rf.changeStateAndReinitialize(FOLLOWER)
	}

	// Rules for Servers->Candidates->3: If AppendEntries RPC received from new leader: convert to follower
	if rf.state == CANDIDATE {
		rf.changeStateAndReinitialize(FOLLOWER)
	}

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		DebugLog(dClient, rf, "eid=%v, receive HB with outdated term from %v; reject", args.EventId, args.LeaderId)
		return
	}
	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// corner case: args.PrevLogIndex==rf.snapshotLastIncludedIndex
	if !(rf.getLastLogIndex() >= args.PrevLogIndex) {
		reply.Success = false
		DebugLog(dClient, rf, "eid=%v, leaderId=%v, !(rf.getLastLogIndex() >= args.PrevLogIndex). logs=%v", args.EventId, args.LeaderId, rf.logs2str(rf.logs))
		return
	}
	var myLogTermAtLeaderPrevLogIndex int
	if rf.logIdx2physicalIdx(args.PrevLogIndex) <= 0 {
		myLogTermAtLeaderPrevLogIndex = max(0, rf.snapshotLastIncludedTerm)
	} else {
		myLogTermAtLeaderPrevLogIndex = rf.logs[rf.logIdx2physicalIdx(args.PrevLogIndex)].Term
	}
	if !(myLogTermAtLeaderPrevLogIndex == args.PrevLogTerm) {
		reply.Success = false
		DebugLog(dClient, rf, "eid=%v, leaderId=%v, !(myLogTermAtLeaderPrevLogIndex == args.PrevLogTerm). logs=%v", args.EventId, args.LeaderId, rf.logs2str(rf.logs))
		return
	}

	// 4. Append any new entries not already in the log
	for i, newLog := range args.Entries {
		newIndex := args.PrevLogIndex + i + 1 // the index of this new log
		newTerm := newLog.Term
		if rf.logIdx2physicalIdx(newIndex) <= 0 {
			continue // skip new logs that are already in the snapshot
		}
		// 3. check if an existing entry conflicts with a new one (same index but different terms)
		if rf.getLastLogIndex() >= newIndex && rf.logs[rf.logIdx2physicalIdx(newIndex)].Term != newTerm {
			// delete the existing entry and all that follow it
			rf.logs = rf.logs[:rf.logIdx2physicalIdx(newIndex)]
		}
		if !(rf.getLastLogIndex() >= newIndex && /*check if "newLog" already exist*/
			rf.logs[rf.logIdx2physicalIdx(newIndex)].Term == newLog.Term && rf.logs[rf.logIdx2physicalIdx(newIndex)].Command == newLog.Command) {
			rf.logs = append(rf.logs, newLog)
		}
		if rf.logs[rf.logIdx2physicalIdx(newIndex)].Term != newTerm {
			panic("rf.logs[newIndex].Term!=newTerm after append!!!????")
		}
	}
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
	}
	DebugLog(dClient, rf, "eid=%v, handle AppendEntries from %v Success= %v, log= %v, STATES: %v",
		args.EventId, args.LeaderId, reply.Success, rf.logs2str(rf.logs), rf.PrintAllIndicesAndTermsStates())

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) findConflictTerm(Xterm int) (bool, int) {
	hasXTerm := false
	lastEntryIdxWithXTerm := -1
	for idx, logItem := range rf.logs {
		if logItem.Term == Xterm {
			hasXTerm = true
			lastEntryIdxWithXTerm = idx
		}
	}
	return hasXTerm, lastEntryIdxWithXTerm /*!!! Note: this is physical index (lab 2D)*/
}

// sendAppendEntriesToOnePeer
//
//	@Description: given AppendEntriesArgs, send heartbeat to a follower. Upon reply, update nextIndex and matchIndex
//	@receiver rf
//	@param peerId: the server you're sending to
//	@param logStartIndex: you want to send rf.logs[logStartIndex:] to the peer
func (rf *Raft) sendAppendEntriesToOnePeer(followerId, nextIdx int, args AppendEntriesArgs, eventId /*for debug*/ string) {
	if rf.killed() {
		DebugLog(dInfo, rf, "sendAppendEntriesToOnePeer, but killed!")
		return
	}
	rf.mutex.Lock()
	rf.persist() //persist before the server issues the next RPC
	rf.mutex.Unlock()
	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(followerId, &args, &reply) {
		rf.mutex.Lock()
		defer rf.mutex.Unlock()
		defer func() { // Rule.All Servers.1
			if rf.commitIndex > rf.lastApplied {
				DebugLog(dInfo, rf, "eid=%v, notify myself apply logs, cmitIdx=%v, appliedIdx=%v", eventId, rf.commitIndex, rf.lastApplied)
				rf.applyCond.Signal() // notify apply logs
			}
		}()
		defer rf.persist() // persist before the server replies to an ongoing RPC.

		if rf.currentTerm != args.Term { // term confusion
			return
		}
		// All Servers.2 RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		if reply.Term > rf.currentTerm {
			DebugLog(dLeader, rf, "follower S%v's term is larger; be follower; refresh", followerId)
			rf.currentTerm = reply.Term
			rf.changeStateAndReinitialize(FOLLOWER) // leader -> follower
			return
		}

		//AppendEntries consistency check fail
		if reply.Success == false {
			if reply.XIndex == -1 && reply.XTerm == -1 && reply.XLen == -1 {
				panic("unexpected: all XMsg==-1")
			}
			if reply.XIndex != -1 && reply.XTerm != -1 {
				// follower has conflicting term
				hasXTerm, lastLogWithXTermPhysicalIdx /*physical index*/ := rf.findConflictTerm(reply.XTerm)
				if hasXTerm {
					rf.nextIndex[followerId] = max(1, rf.physicalIdx2logIdx(lastLogWithXTermPhysicalIdx))
				} else {
					rf.nextIndex[followerId] = max(1, reply.XIndex /*! it is expected that XIndex is original log index*/)
				}
				DebugLog(dLeader, rf, "eid=%v, (failed) follower=%v  hasXTerm=%v, lastLogWithXTermIdx=%v", eventId, followerId, hasXTerm, rf.physicalIdx2logIdx(lastLogWithXTermPhysicalIdx))
			} else {
				// follower too short!
				DebugLog(dLeader, rf, "eid=%v, (failed) follower=%v too short! Xlen=%v", eventId, followerId, reply.XLen)

				rf.nextIndex[followerId] = max(1, reply.XLen /*! it is expected that XLen is original log index*/)
			}
			DebugLog(dLeader, rf, "eid=%v, AppendEntries consistency check fail, nextIndex[%v]--, =%v", eventId, followerId, rf.nextIndex[followerId])
		} else {
			// AppendEntries will succeed, which removes any conflicting entries in the follower’s log and appends entries from the leader’s log
			// reset nextIndex[peerId]
			rf.nextIndex[followerId] = nextIdx + len(args.Entries)
			rf.matchIndex[followerId] = rf.nextIndex[followerId] - 1
			DebugLog(dLeader, rf, "eid=%v, AppendEntries consistency check success, nextIndex[%v]=%v", eventId, followerId, rf.nextIndex[followerId])
		}

		// If there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
		for N := rf.commitIndex + 1; N <= rf.getLastLogIndex(); N++ {
			count := 1 // number of peers that "matchIndex[i] ≥ N"
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N {
					count++
				}
			}
			if float64(count)/float64(len(rf.peers)) > 0.5 /*majority  committed*/ {
				if rf.logs[rf.logIdx2physicalIdx(N)].Term == rf.currentTerm {
					DebugLog(dLeader, rf, "eid=%v, majority received! update leader commitIndex =%v", eventId, N)
					rf.commitIndex = N
				}
			}
		}
	}
}

// sendAppendEntriesToAllPeers
//
//	@Description: send snapshot (when the reqired logs are in snapshot), new entries or empty entires to peers
//	@receiver rf
func (rf *Raft) sendAppendEntriesToAllPeers() {
	if rf.killed() {
		DebugLog(dInfo, rf, "sendAppendEntriesToAllPeers, but killed!")
		return
	}
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	DebugLog(dLeader, rf, "AppendEntries triggered, send to all peers")
	if rf.state != LEADER {
		return
	}
	eid := generateEventId(rf) // an id represent the event of sending HBs to all peers
	for followerId := 0; followerId < len(rf.peers); followerId++ {
		if followerId != rf.me {
			leaderLastLogIdx := rf.getLastLogIndex()
			nextIdx := rf.nextIndex[followerId]
			if rf.logIdx2physicalIdx(nextIdx) <= 0 { // it doesn't have the logs; send snapshot
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.snapshotLastIncludedIdx,
					LastIncludedTerm:  rf.snapshotLastIncludedTerm,
					Data:              rf.snapshot,
				}
				DebugLog(dLeader, rf, "nextIdx=%v, sent snapshot to %v, eid=%v,  cmitIdx=%v, appliedIdx=%v, lastLogIdx=%v, ( firstLogIdx=%v, splastIcIdx=%v, splastIcT=%v ), nextIndex[%v]=%v, currLogs= %v", nextIdx, followerId, eid, rf.commitIndex, rf.lastApplied, leaderLastLogIdx, rf.firstLogIdx, rf.snapshotLastIncludedIdx, rf.snapshotLastIncludedTerm, followerId, nextIdx, rf.logs2str(rf.logs))
				go rf.sendInstallSnapshotToOnePeer(followerId, args)
			} else if leaderLastLogIdx >= nextIdx { // need to send new logs (rf.logs[nextIdx:]) to followers
				DebugLog(dLeader, rf, "sent AppendEntries to %v, eid=%v,  cmitIdx=%v, appliedIdx=%v, lastLogIdx=%v, ( firstLogIdx=%v, splastIcIdx=%v, splastIcT=%v ), nextIndex[%v]=%v, currLogs= %v", followerId, eid, rf.commitIndex, rf.lastApplied, leaderLastLogIdx, rf.firstLogIdx, rf.snapshotLastIncludedIdx, rf.snapshotLastIncludedTerm, followerId, nextIdx, rf.logs2str(rf.logs))
				var preLogTerm int
				if rf.logIdx2physicalIdx(nextIdx-1) <= 0 {
					preLogTerm = max(0, rf.snapshotLastIncludedTerm /*-1 when it is first initialized*/)
				} else {
					preLogTerm = rf.logs[rf.logIdx2physicalIdx(nextIdx-1)].Term
				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: nextIdx - 1, // index of log entry immediately preceding new ones
					PrevLogTerm:  preLogTerm,  //rf.logs[rf.logIdx2physicalIdx(nextIdx-1)].Term, // term of PrevLogIndex Log // todo nextIdx=19, firstLogIdx=20
					Entries:      rf.logs[rf.logIdx2physicalIdx(nextIdx):],
					LeaderCommit: rf.commitIndex,
					EventId:      eid, // only for debug
				}
				DebugLog(dLeader, rf, "+ args=%v", args.toString())
				go rf.sendAppendEntriesToOnePeer(followerId, nextIdx, args, eid)
			} else { // no new logs are needed; send empty logs entries as heartbeat
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.getLastLogIndex(), // last log index
					PrevLogTerm:  rf.getLastLogTerm(),  // term of last log
					Entries:      make([]Log, 0),       // empty logs
					LeaderCommit: rf.commitIndex,
					EventId:      eid, // only for debug
				}
				DebugLog(dLeader, rf, "sent HB to %v, eid=%v,  cmitIdx=%v, appliedIdx=%v, lastLogIdx=%v, ( firstLogIdx=%v, splastIcIdx=%v, splastIcT=%v ), nextIndex[%v]=%v, currLogs= %v", followerId, eid, rf.commitIndex, rf.lastApplied, leaderLastLogIdx, rf.firstLogIdx, rf.snapshotLastIncludedIdx, rf.snapshotLastIncludedTerm, followerId, nextIdx, rf.logs2str(rf.logs))
				go rf.sendAppendEntriesToOnePeer(followerId, nextIdx, args, eid)
			}
		}
	}

}
