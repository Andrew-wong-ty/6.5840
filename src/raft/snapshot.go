package raft

/****************************************************  InstallSnapshot  *******************************************************/

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // leader id
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshotToOnePeer(peerId int, args InstallSnapshotArgs) {
	rf.mutex.Lock()
	rf.persist() //persist before the server issues the next RPC
	rf.mutex.Unlock()
	reply := InstallSnapshotReply{}
	if rf.killed() == false && rf.sendInstallSnapshot(peerId, &args, &reply) {
		rf.mutex.Lock()
		defer rf.mutex.Unlock()
		defer rf.persist()

		// All Servers.2: RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.changeStateAndReinitialize(FOLLOWER) // leader -> follower
			return
		}

		if args.Term != rf.currentTerm || rf.state != LEADER { // things changed
			return
		}

		// update nextIndex and matchedIndex
		rf.nextIndex[peerId] = args.LastIncludedIndex + 1
		rf.matchIndex[peerId] = args.LastIncludedIndex
		DebugLog(dSnap, rf, "install snapshot on S%v succeed", peerId)
	}
}

// InstallSnapshot is the handler for the RPC call from other machine to install a snapshot on this machine
/*** RPC handler ***/
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	defer rf.persist()
	reply.Term = max(args.Term, rf.currentTerm)
	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// All Servers.2: RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if reply.Term > rf.currentTerm {
		DebugLog(dLeader, rf, "follower S%v's term is larger; be follower; refresh", args.LeaderId)
		rf.currentTerm = reply.Term
		rf.changeStateAndReinitialize(FOLLOWER) // leader -> follower
	}
	if rf.snapshot != nil && rf.snapshotLastIncludedIdx > args.LastIncludedIndex {
		return // reject stale snapshot (not mentioned in paper)
	}

	/*
	 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	 6. If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	 7. Discard the entire log
	 8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	*/
	//update rf.logs
	flag6 := false
	for logIdx, log := range rf.logs {
		if logIdx == 0 {
			continue
		}
		if rf.physicalIdx2logIdx(logIdx) == args.LastIncludedIndex && log.Term == args.LastIncludedTerm {
			DebugLog(dSnap, rf, "handle InstallSnapshot from%v; included_Idx found: %v, lastLog_Idx=%v", args.LeaderId, rf.physicalIdx2logIdx(logIdx), rf.getLastLogIndex())
			flag6 = true
			break
		}
	}

	if flag6 { // do 6
		newFirstLogIdx := args.LastIncludedIndex + 1
		newLogs := make([]Log, 1)
		newLogs = append(newLogs, rf.logs[rf.logIdx2physicalIdx(newFirstLogIdx):]...)
		rf.logs = newLogs

		// when the whole log is cleared, also do necessary 8
		if len(newLogs) == 1 {
			rf.commitIndex = args.LastIncludedIndex
			rf.lastApplied = args.LastIncludedIndex
		} else {
			rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
			rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
		}

	} else { // do 7 & 8
		// do 7
		rf.logs = make([]Log, 1)
		// do 8
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
	}
	// update snapshot states
	rf.snapshot = args.Data
	rf.snapshotLastIncludedIdx = args.LastIncludedIndex
	rf.snapshotLastIncludedTerm = args.LastIncludedTerm
	rf.firstLogIdx = args.LastIncludedIndex + 1

	DebugLog(dSnap, rf, "handle InstallSnapshot from%v; retain=%v, STATES: %v", args.LeaderId, flag6, rf.PrintAllIndicesAndTermsStates())

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.snapshotLastIncludedTerm,
		SnapshotIndex: rf.snapshotLastIncludedIdx,
	}
	go func() {
		rf.applyChan <- msg
	}()
}

// Snapshot
//
//	@Description: Called by the service to package the logs[:index] to be a snapshot. Then delete the logs
//	The log slice's new index would be index+1
//	@param index the rf.logs[:index] will be compacted to be a snapshot
//	@param snapshot the bytes of the snapshot (i.e., the machine's state after applying rf.logs[:index])
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Task: trim logs to logs[index+1:], update firstLogIdx, and snapshotLastIncluded{Idx/Term}
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	//! Note: the log having the firstLogIdx is non exist; consider refactor name:  rf.firstLogIdx -> rf.expectedFirstLogIdx
	if rf.firstLogIdx <= index /*assure this log index could be found in rf.logs*/ {
		// snapshot parameters
		newFirstLogIdx := index + 1
		lastIncludedIdx := index
		physicalIdx := rf.logIdx2physicalIdx(index)
		if physicalIdx <= 0 || physicalIdx >= len(rf.logs) {
			return // the index may be too advanced. For example, 1stLogIndex = 98, index = 99, logLen = 2 (highest log index=98)
		}
		lastIncludedTerm := rf.logs[rf.logIdx2physicalIdx(index)].Term
		// generate new logs
		newLogs := make([]Log, 1) // don't forget the dummy one
		newLogs[0].Command, newLogs[0].Term = nil, 0
		newLogs = append(newLogs, rf.logs[rf.logIdx2physicalIdx(newFirstLogIdx):]...)

		rf.logs = newLogs
		rf.firstLogIdx = newFirstLogIdx

		rf.snapshot = snapshot
		rf.snapshotLastIncludedIdx = lastIncludedIdx
		rf.snapshotLastIncludedTerm = lastIncludedTerm
		DebugLog(dSnap, rf, "snapshot called!! idx=%v, STATES: %v", index, rf.PrintAllIndicesAndTermsStates())
		rf.persist()
	} else {
		DebugLog(dSnap, rf, "snapshot: invalid index")
	}

}

// physicalIdx2logIdx
//
//	@Description: Convert physical index in rf.logs to log Index.
//	  This is because after calling the Snapshot, some of the logs are discarded and
//	  the indices in the log slice no longer represent the log index.
//	@receiver rf
//	@param index: physical index (i.e., element's index in rf.logs slice)
//	@return int: log index
func (rf *Raft) physicalIdx2logIdx(index int) int {
	if index == 0 {
		return 0
	} else {
		return index + rf.firstLogIdx - 1
	}
}

// logIdx2physicalIdx
//
//	@Description: Convert log index to physical index.
//	@receiver rf
//	@param index: log index
//	@return int: physical index (i.e., element's index in rf.logs slice)
func (rf *Raft) logIdx2physicalIdx(index int) int {
	if index == 0 {
		return 0
	} else {
		res := index - rf.firstLogIdx + 1
		if res <= 0 || res >= len(rf.logs) {
			DebugLog(dError, rf, "possible invalid physical Idx: idx=%v, STATES:%v", index, rf.PrintAllIndicesAndTermsStates())
		}
		return res
	}
}

// getLastLogIndex
//
//	@Description: get the index of server's last log entry
//	@receiver rf
//	@return int 0 or last log's index
func (rf *Raft) getLastLogIndex() int { // original log Index
	if len(rf.logs) == 1 {
		// rf.logs is empty, which only contains the dummynode Log{nil,0}.
		// The reasons may be: Case 1. It hasn't reveived any logs. Case 2. Snapshot is created and these logs are discarded
		if rf.firstLogIdx == 1 { // Case 1
			return 0
		} else { // Case 2
			return rf.snapshotLastIncludedIdx
		}
	} else {
		// rf.logs is non-empty
		return rf.physicalIdx2logIdx(len(rf.logs) - 1)
	}

}

// term of server’s last log entry
func (rf *Raft) getLastLogTerm() int {
	if len(rf.logs) == 1 {
		// rf.logs is empty, which only contains the dummy node Log{nil,0}.
		// The reasons may be: Case 1. It hasn't received any logs. Case 2. Snapshot is created and these logs are discarded
		if rf.firstLogIdx == 1 { // Case 1
			return 0
		} else { // Case 2
			return rf.snapshotLastIncludedTerm
		}
	} else {
		return rf.logs[len(rf.logs)-1].Term // The term of physical last log
	}
}
