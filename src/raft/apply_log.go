package raft

// applyCommittedLogs
//
//	@Description: A goroutine that apply committed logs. It will wait until appliedIdx<committedIdx
//	@receiver rf
func (rf *Raft) applyCommittedLogs() {
	for rf.killed() == false {
		rf.mutex.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait() // wait until appliedIdx < committedIdx; and acquire lock again
		}
		index := rf.lastApplied + 1
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.logIdx2physicalIdx(index)].Command,
			CommandIndex: index,
		}
		rf.lastApplied++
		DebugLog(dCommit, rf, "committed {Idx=%v, T=%v, Cmd=%v}, lastApp=%v, cmitIdx=%v",
			index, rf.logs[rf.logIdx2physicalIdx(index)].Term, rf.logs[rf.logIdx2physicalIdx(index)].Command, rf.lastApplied, rf.commitIndex)
		rf.mutex.Unlock()
		rf.applyChan <- msg // To prevent possible deadlock, put this outside the lock context
	}
}
