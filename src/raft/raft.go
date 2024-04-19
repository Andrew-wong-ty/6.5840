package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// tester limits you tens of heartbeats per second
//const heartbeatTimeout time.Duration = 100 * time.Millisecond //ok 50

// ApplyMsg : as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int // the index of log

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Command interface{}
	Term    int // first index is 1
}

type LeaderState struct {
	nextIndex  []int
	matchIndex []int
}

type Role string

const (
	FOLLOWER  Role = "follower"
	CANDIDATE      = "candidate"
	LEADER         = "leader"
)

// Raft a single Raft peer.
type Raft struct {
	mutex     sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//! persistent states
	currentTerm int
	votedFor    *int // todo: change its type to int (-1)
	logs        []Log

	//! volatile states (all server)
	commitIndex int // index of highest log entry known to be committed (done)
	lastApplied int // index of highest log entry applied to state machine
	//! volatile states (leader)
	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leaderlast log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated (done) on server

	state Role // roles: {follower/candidate/leader}

	//! for leader election,
	electionTimeoutTicker *time.Ticker

	//! for managing heartbeat
	heartbeatTimeoutTicker *time.Ticker

	//  for notify apply
	applyCond *sync.Cond
	applyChan chan ApplyMsg

	//! lab 2d
	firstLogIdx              int    // the log index of the first Log item in rf.logs
	snapshot                 []byte // this raft server's most recent snapshot
	snapshotLastIncludedIdx  int
	snapshotLastIncludedTerm int

	//! Timeouts
	heartbeatTimeout time.Duration
}

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
				rf.nextIndex[i] = rf.getLastLogIndex() + 1
				rf.matchIndex[i] = 0
			}
			// immediately trigger a heartbeat
			rf.heartbeatTimeoutTicker.Reset(rf.heartbeatTimeout)
			go rf.sendAppendEntriesToAllPeers()
			// 3A: immediately commit a no-op log after being a leader?
			//go rf.Start("NOOP")
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

func (rf *Raft) resetElectionTicker() time.Duration {
	duration := randomElectionDuration()
	rf.electionTimeoutTicker.Reset(duration)
	return duration
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	rf.mutex.Lock()
	term = rf.currentTerm
	isLeader = rf.state == LEADER
	rf.mutex.Unlock()
	return term, isLeader
}

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
		dbgIdx := rf.logIdx2physicalIdx(index)
		if dbgIdx <= 0 || dbgIdx >= len(rf.logs) {
			DebugLog(dError, rf, "apply invalid physical Idx: idx=%v, STATES:%v", index, rf.printAllIndicesAndTermsStates())
			errMsg := fmt.Sprintf("error: invalid physical index, indx=%v, STATES:%v", index, rf.printAllIndicesAndTermsStates())
			panic(errMsg)
		}
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.logIdx2physicalIdx(index)].Command, //! possible TestSnapshotRecoverManyClients3B bug here: invalid index
			CommandIndex: index,
		}
		rf.lastApplied++
		DebugLog(dCommit, rf, "committed {Idx=%v, T=%v, Cmd=%v}, lastApp=%v, cmitIdx=%v",
			index, rf.logs[rf.logIdx2physicalIdx(index)].Term, rf.logs[rf.logIdx2physicalIdx(index)].Command, rf.lastApplied, rf.commitIndex)
		rf.mutex.Unlock()
		rf.applyChan <- msg // To prevent possible deadlock, put this outside the lock context
	}
}

func (rf *Raft) SetHeartbeatTimeout(newTimeout time.Duration) {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	rf.heartbeatTimeout = newTimeout
}

// Start an agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// 1. The first return value is the index that the Command will appear at
// if it's ever committed.
// 2. The second return value is the current term.
// 3. The third return value is true if this server believes it is the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	// Your code here (2B).
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	if rf.state != LEADER || rf.killed() {
		return index, term, false
	}
	// append to logs
	rf.logs = append(rf.logs, Log{Command: command, Term: rf.currentTerm})
	//rf.persist()
	term = rf.currentTerm
	index = rf.getLastLogIndex()
	DebugLog(dLog, rf, "new log arrives {idx=%v, t=%v, cmd=%v}", index, term, convertCommandToString(command))

	/*
		Note: don't do the following
			rf.heartbeatTimeoutTicker.Reset(rf.heartbeatTimeout)
			go rf.sendAppendEntriesToAllPeers()
		immediately sent AppendEntries to followers will case possible bugs:
		1. TestSnapshotBasic2D: multiple commits follow an install-snapshot, cause states can not be canceled
		2. bugs in TestSnapshotRecoverManyClients3B, possible out of index bug
	*/
	return index, term, true
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should Stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DebugLog(dInfo, rf, "is set killed")
	// Your code here, if desired.
	rf.electionTimeoutTicker.Stop()
	rf.heartbeatTimeoutTicker.Stop()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker is goroutine that receives msg from the election ticker and appendEntries ticker, and do correspondent actions
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimeoutTicker.C:
			go rf.electMyselfToBeLeader()

		case <-rf.heartbeatTimeoutTicker.C:
			go rf.sendAppendEntriesToAllPeers()

		}
	}
	DebugLog(dError, rf, "ticker ends since raft is killed")
}

// Make creates a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	heartbeatTimeout := 60 * time.Millisecond // set it to be  >=50 ms to pass tests
	rf := &Raft{
		mutex:                    sync.Mutex{},
		peers:                    peers,
		persister:                persister,
		me:                       me,
		dead:                     0,
		currentTerm:              0,
		votedFor:                 nil,
		logs:                     make([]Log, 1), // since log starts with index 1, the first element is a dummy one
		commitIndex:              0,
		lastApplied:              0,
		nextIndex:                make([]int, len(peers)),
		matchIndex:               make([]int, len(peers)),
		state:                    FOLLOWER,
		electionTimeoutTicker:    time.NewTicker(randomElectionDuration()),
		heartbeatTimeoutTicker:   time.NewTicker(heartbeatTimeout),
		applyChan:                applyCh,
		firstLogIdx:              1,
		snapshot:                 nil,
		snapshotLastIncludedTerm: -1,
		snapshotLastIncludedIdx:  -1,
	}
	rf.heartbeatTimeout = heartbeatTimeout
	rf.applyCond = sync.NewCond(&rf.mutex)
	rf.heartbeatTimeoutTicker.Stop() // wait until being a leader
	rf.electionTimeoutTicker.Stop()

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start the goroutine to commit logs
	go rf.applyCommittedLogs()
	//go rf.sendHeartbeats()
	DebugLog(dInfo, rf, "Raft server is made, nPeers=%v", len(peers))
	// start election ticker
	rf.electionTimeoutTicker.Reset(randomElectionDuration())
	return rf
}
