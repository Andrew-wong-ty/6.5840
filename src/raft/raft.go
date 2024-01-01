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
	"strconv"

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// tester limits you tens of heartbeats per second (20-90 heartbeat/second)
const HEARTBEATTIMEOUT time.Duration = 80 * time.Millisecond

// as each Raft peer becomes aware that successive log entries are
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
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	command string
	term    int
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mutex     sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//! persistent states
	currentTerm int
	votedFor    *int
	logs        []Log

	commitIndex int  // index of highest log entry known to be committed (done)
	lastApplied int  // index of highest log entry applied to state machine
	state       Role // follower/candidate/leader

	//! for leader election,
	electionTimeoutTicker  *time.Ticker // trigger election periodically
	heartbeatTimeoutTicker *time.Ticker // trigger sendAppendEntries periodically
}

// changeStateAndReinitialize
//
//	@Description: transit state between roles of follower/candidate/leader,
//	and then reinitialize and do after effects  (e.g. reset/stop ticker, reallocate nextIndex and matchIndex)
//	@receiver rf
//	@param role: initial role {follower/candidate/leader}
func (rf *Raft) changeStateAndReinitialize(role Role) {
	if rf.state == role {
		return
	}
	DebugLog(dInfo, rf, "role transfer from %v to %v, stop heartbeat", string(rf.state), string(role))
	// leader -> follower/candidate
	if rf.state == LEADER {
		switch role {
		case FOLLOWER:
			rf.state = FOLLOWER
			rf.heartbeatTimeoutTicker.Stop()
			rf.resetElectionTimeoutTicker()
			break
		case CANDIDATE:
			rf.state = CANDIDATE
			rf.heartbeatTimeoutTicker.Stop()
			rf.resetElectionTimeoutTicker()
			break
		}
	} else { // follower/candidate -> {follower/candidate/leader}
		switch role {
		case LEADER:
			rf.state = LEADER
			rf.electionTimeoutTicker.Stop()
			// send HB immediately, and reset ticker
			go rf.sendAppendEntriesToAllPeers()
			rf.heartbeatTimeoutTicker.Reset(HEARTBEATTIMEOUT)
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

func (rf *Raft) resetElectionTimeoutTicker() time.Duration {
	duration := randomElectionDuration()
	rf.electionTimeoutTicker.Reset(duration)
	return duration
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs)
}
func (rf *Raft) getLastLog() Log {
	if len(rf.logs) == 0 {
		return Log{
			command: "",
			term:    0,
		}
	}
	return rf.logs[len(rf.logs)-1]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// todo Your code here (2A).
	rf.mutex.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mutex.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler. (reciver's handler)
// handle the vote request sent from peers
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	defer DebugLog(dVote, rf, "my role=%v, handled RequestVote from S%v grant= %v", string(rf.state), args.CandidateId, reply.VoteGranted)

	reply.Term = max(rf.currentTerm, args.Term)

	// RequestVote RPC->Receiver implementation->1: Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	// Rules for Servers->All Servers->2: If RPC request or response contains term T > currentTerm: set currentTerm = T,
	// convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = nil
		rf.changeStateAndReinitialize(FOLLOWER)
	}

	// RequestVote RPC->Receiver implementation->2: If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if rf.votedFor == nil || *(rf.votedFor) == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = &args.CandidateId
	}

	if reply.VoteGranted {
		rf.resetElectionTimeoutTicker()
	}

}

type Entry struct {
	Command string
	Term    int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC handler (for receivers)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// for 2A, only reset timeout
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	reply.Term = max(rf.currentTerm, args.Term)
	reply.Success = true

	// Rules for Servers->All Servers->2
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = nil
		rf.changeStateAndReinitialize(FOLLOWER)
	}

	// Rules for Servers.Candidates.3 If AppendEntries RPC received from new leader: convert to follower
	if rf.state == CANDIDATE {
		rf.changeStateAndReinitialize(FOLLOWER)
	}

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	DebugLog(dClient, rf, "handle AppendEntries from %v Success= %v, refresh election timeout", args.LeaderId, reply.Success)
	rf.resetElectionTimeoutTicker()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendAppendEntriesToAllPeers() {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	DebugLog(dLeader, rf, "sendHeartbeats triggered, send to all peers")
	if rf.state != LEADER {
		return
	}
	// *only for debug* an id represent the event of sending HBs to all peers
	eid := generateRandomString(6) + "_T" + strconv.Itoa(rf.currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			peerId := i
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []Entry{},
				LeaderCommit: 0,
			}
			reply := AppendEntriesReply{}
			go func(eventId string) {
				DebugLog(dLeader, rf, "eid=%v, send AppendEntries to S%v ", eid, peerId)
				if rf.sendAppendEntries(peerId, &args, &reply) {
					rf.mutex.Lock()
					// Rules for Servers->All Servers->2
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.changeStateAndReinitialize(FOLLOWER)
						rf.resetElectionTimeoutTicker()
					}
					rf.mutex.Unlock()
				}
			}(eid)
		}
	}

}

func (rf *Raft) generateRequestVoteArgs() RequestVoteArgs {
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLog().term,
	}
	return args
}

func (rf *Raft) electMyselfToBeLeader() {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	defer rf.resetElectionTimeoutTicker()
	DebugLog(dVote, rf, "election timeout! start election!")
	// leader cannot start election
	if rf.state == FOLLOWER || rf.state == CANDIDATE {
		rf.changeStateAndReinitialize(CANDIDATE)
		rf.currentTerm++
		rf.votedFor = &rf.me
		// rf.mu protect the following variables
		voteCount := 1
		// once it is a leader, election done
		electionDone := false
		// copy the curr Raft Server States
		args := rf.generateRequestVoteArgs()
		// eventId to mark this requestVote
		eventId := generateEventId(5, rf)
		// sendRequestVote to all peers in parallel
		DebugLog(dVote, rf, "sendRequestVote to all peers")
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(peerId int) { // pass the copied value
				reply := RequestVoteReply{}
				if rf.sendRequestVote(peerId, &args, &reply) {
					rf.mutex.Lock()
					defer rf.mutex.Unlock()
					if electionDone {
						return
					}
					//response contains term T > currentTerm: set currentTerm = T, convert to follower
					if reply.Term > rf.currentTerm {
						rf.changeStateAndReinitialize(FOLLOWER)
						rf.currentTerm = reply.Term
					}
					if args.Term == rf.currentTerm && // everything remains the same
						args.LastLogIndex == rf.getLastLogIndex() &&
						args.LastLogTerm == rf.getLastLog().term &&
						rf.state == CANDIDATE /*still electing*/ {
						if reply.VoteGranted {
							voteCount++
						}
					}
					DebugLog(dVote, rf, "eid=%v vote=%v/%v", eventId, voteCount, len(rf.peers))

					if float64(voteCount)/float64(len(rf.peers)) > 0.5 {
						DebugLog(dVote, rf, "eid=%v, it is a leader now! send heartbeat! ", eventId)
						electionDone = true
						rf.changeStateAndReinitialize(LEADER)
					}
				}
			}(i)
		}
	}

}

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

// the service or tester wants to create a Raft server. the ports
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
		mutex:                  sync.Mutex{},
		peers:                  peers,
		persister:              persister,
		me:                     me,
		dead:                   0,
		currentTerm:            0,
		votedFor:               nil,
		logs:                   make([]Log, 0),
		commitIndex:            0,
		lastApplied:            0,
		state:                  FOLLOWER,
		electionTimeoutTicker:  time.NewTicker(randomElectionDuration()),
		heartbeatTimeoutTicker: time.NewTicker(HEARTBEATTIMEOUT),
	}

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.heartbeatTimeoutTicker.Stop() // wait until being a leader
	// start ticker goroutine to start elections / send heartbeat
	go rf.ticker()
	DebugLog(dInfo, rf, "Raft server is made, nPeers=%v", len(peers))
	return rf
}
