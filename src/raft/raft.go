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
	"math/rand"
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

type Role string

// Raft a single Raft peer.
type Raft struct {
	mutex                    sync.RWMutex        // a lock to protect the followings
	peers                    []*labrpc.ClientEnd // RPC end points of all peers
	persister                *Persister          // Object to hold this peer's persisted state
	me                       int                 // this peer's index into peers[]
	state                    Role                // this machine's role: {follower/candidate/leader}
	dead                     int32               // set by Kill()
	currentTerm              int                 // @ persistent-state
	votedFor                 *int                // @ persistent-state
	logs                     []Log               // @ persistent-state
	commitIndex              int                 // @ volatile-state-all-server; index of highest log entry known to be committed
	lastApplied              int                 // @ volatile-state-all-server; index of highest log entry applied to state machine
	nextIndex                []int               // for each server, index of the next log entry to send to that server (initialized to leader's last log index + 1)
	matchIndex               []int               // for each server, index of highest log entry known to be replicated (done) on server
	electionTimeoutTicker    *time.Ticker        // a ticker to notify election timeout
	heartbeatTimeoutTicker   *time.Ticker        // a ticker to notify send AppendEntries to followers
	applyCond                *sync.Cond          // Condition variable to wait for committedIndex > lastApplied
	applyChan                chan ApplyMsg       // Send the applied ApplyMsg to the services (kvraft/tester)
	firstLogIdx              int                 // @Snapshot; the log index of the first Log item in rf.logs
	snapshot                 []byte              // @Snapshot; this raft server's most recent snapshot
	snapshotLastIncludedIdx  int                 // @Snapshot the latest snapshot's lasted included log's index
	snapshotLastIncludedTerm int                 // @Snapshot; the latest snapshot's lasted included log's term
	heartbeatTimeout         int64               // send AppendEntries(heartbeat) to followers every heartbeatTimeout ms
	electionTimeout          int64               // election timeout in millisecond
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	rf.mutex.RLock()
	term = rf.currentTerm
	isLeader = rf.state == LEADER
	rf.mutex.RUnlock()
	return term, isLeader
}

// SetHeartbeatTimeout set a new heartbeat timeout
func (rf *Raft) SetHeartbeatTimeout(newTimeoutMillisecond int64) {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	rf.heartbeatTimeout = newTimeoutMillisecond
}

// Start an agreement on the next Command to be appended to Raft's log.
//
// if this server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// 1. The first return value is the index that the Command will appear at
// if it's ever committed.
// 2. The second return value is the current term.
// 3. The third return value is true if this server believes it is the leader.
// ! Note: the command's type must be comparable
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
			Note: don't immediately start send AppendEntries
				rf.heartbeatTimeoutTicker.Reset(rf.heartbeatTimeout)
				go rf.sendAppendEntriesToAllPeers()
			immediately sent AppendEntries to followers will case possible bugs:
		    0. The concurrency of install snapshot (AppendEntries) and sync logs (AppendEntries), undefined behavior.
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
	rf := &Raft{
		mutex:                    sync.RWMutex{},
		peers:                    peers,
		persister:                persister,
		me:                       me,
		dead:                     0,
		currentTerm:              0,
		votedFor:                 nil,
		logs:                     make([]Log, 1), // log starts with index 1
		commitIndex:              0,
		lastApplied:              0,
		nextIndex:                make([]int, len(peers)),
		matchIndex:               make([]int, len(peers)),
		state:                    FOLLOWER,
		applyChan:                applyCh,
		firstLogIdx:              1,
		snapshot:                 nil,
		snapshotLastIncludedTerm: -1,
		snapshotLastIncludedIdx:  -1,
		heartbeatTimeout:         60, // should be  >=50 ms to pass tests
		electionTimeout:          500,
	}
	rf.electionTimeoutTicker = time.NewTicker(time.Duration(rf.electionTimeout+(rand.Int63()%50)) * time.Millisecond)
	rf.heartbeatTimeoutTicker = time.NewTicker(time.Duration(rf.heartbeatTimeout) * time.Millisecond)
	rf.applyCond = sync.NewCond(&rf.mutex)
	rf.heartbeatTimeoutTicker.Stop() // start the ticker only when it is the leader
	rf.electionTimeoutTicker.Stop()

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before the crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start the goroutine to commit logs
	go rf.applyCommittedLogs()
	// start election ticker
	rf.resetElectionTicker()
	DebugLog(dInfo, rf, "Raft server is made, nPeers=%v, electionTimeout=%v, appendEntriesTimeout=%v", len(peers), rf.electionTimeout, rf.heartbeatTimeout)

	return rf
}
