package raft

import (
	"6.5840/labgob"
	"bytes"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		panic("persist encode error")
	}
	votedForVal := -1
	if rf.votedFor != nil {
		votedForVal = *rf.votedFor
	}
	err = e.Encode(votedForVal)
	if err != nil {
		panic("persist encode error")
	}
	err = e.Encode(rf.firstLogIdx)
	if err != nil {
		panic("persist encode error")
	}
	err = e.Encode(rf.snapshotLastIncludedIdx)
	if err != nil {
		panic("persist encode error")
	}
	err = e.Encode(rf.snapshotLastIncludedTerm)
	if err != nil {
		panic("persist encode error")
	}
	err = e.Encode(rf.logs)
	if err != nil {
		panic("persist encode error")
	}
	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.snapshot)
	DebugLog(dPersist, rf, "Persist done")
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedForVal int
	var firstLogIndex int
	var snapshotLastIncludedIdx int
	var snapshotLastIncludedTerm int
	var logs []Log
	err := d.Decode(&currentTerm)
	if err != nil {
		panic("persist decode error")
	}
	err = d.Decode(&votedForVal)
	if err != nil {
		panic("persist decode error")
	}
	err = d.Decode(&firstLogIndex)
	if err != nil {
		panic("persist decode error")
	}
	err = d.Decode(&snapshotLastIncludedIdx)
	if err != nil {
		panic("persist decode error")
	}
	err = d.Decode(&snapshotLastIncludedTerm)
	if err != nil {
		panic("persist decode error")
	}
	err = d.Decode(&logs)
	if err != nil {
		panic("persist decode error")
	}
	rf.currentTerm = currentTerm
	if votedForVal == -1 {
		rf.votedFor = nil
	} else {
		rf.votedFor = &votedForVal
	}
	rf.firstLogIdx = firstLogIndex
	rf.logs = logs
	rf.snapshot = rf.persister.ReadSnapshot()
	rf.snapshotLastIncludedIdx = snapshotLastIncludedIdx
	rf.snapshotLastIncludedTerm = snapshotLastIncludedTerm
	// since all logs in snapshot is committed and applied, we should update the following.
	rf.commitIndex = max(0, snapshotLastIncludedIdx) // snapshotLastIncludedIdx's initial value is -1
	rf.lastApplied = max(0, snapshotLastIncludedIdx)

	DebugLog(dPersist, rf, "read persist data success, T=%v, voteFor=%v, 1stlogIdx=%v, ssLastIdx=%v, ssLastTerm=%v logs=%v", rf.currentTerm, votedForVal, rf.firstLogIdx, rf.snapshotLastIncludedIdx, rf.snapshotLastIncludedTerm, rf.logs2str(logs))

}
