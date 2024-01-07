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
	"6.5840/labgob"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// tester limits you tens of heartbeats per second
const HEARTBEATTIMEOUT time.Duration = 30 * time.Millisecond

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

// A Go object implementing a single Raft peer.
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
			// reinitializes matchIndex[] and nextIndex[]
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.getLastLogIndex() + 1
				rf.matchIndex[i] = 0
			}
			// immediately trigger a heartbeat
			rf.heartbeatTimeoutTicker.Reset(HEARTBEATTIMEOUT)
			go rf.sendAppendEntriesToAllPeers()
			// 3A: immediately commit a no-op log after being a leader?
			go rf.Start("NOOP")
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
		if idx == 0 {
			res += fmt.Sprintf("{idx=%v t=%v cmd=%v}, ...", idx, logItem.Term, convertCommandToString(logItem.Command))
		}
		if idx >= l-1-5 {
			res += fmt.Sprintf("{idx=%v t=%v cmd=%v}, ", idx, logItem.Term, convertCommandToString(logItem.Command))

		}
	}
	return "[" + res + "]"
}

// getLastLogIndex
//
//	@Description: get the index of server's last log entry
//	@receiver rf
//	@return int
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	votedForVal := -1
	if rf.votedFor != nil {
		votedForVal = *rf.votedFor
	}
	e.Encode(votedForVal)
	e.Encode(rf.firstLogIdx)
	e.Encode(rf.snapshotLastIncludedIdx)
	e.Encode(rf.snapshotLastIncludedTerm)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
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
		return
	}
	err = d.Decode(&votedForVal)
	if err != nil {
		return
	}
	err = d.Decode(&firstLogIndex)
	if err != nil {
		return
	}
	err = d.Decode(&snapshotLastIncludedIdx)
	if err != nil {
		return
	}
	err = d.Decode(&snapshotLastIncludedTerm)
	if err != nil {
		return
	}
	err = d.Decode(&logs)
	if err != nil {
		return
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

/****************************************************  RequestVote  *******************************************************/

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
/*** handler ***/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	defer rf.persist() //persisted before the server replies to an ongoing RPC.

	reply.Term = max(rf.currentTerm, args.Term)
	reply.VoteGranted = false

	// RequestVote RPC->Receiver implementation->1: Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		DebugLog(dVote, rf, "my role=%v, handled reqVote from %v grant= %v", string(rf.state), args.CandidateId, reply.VoteGranted)
		return
	}

	// Rules for Servers->All Servers->2: If RPC request or response contains term T > currentTerm: set currentTerm = T,
	// convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		DebugLog(dVote, rf, "my role=%v, his(S%v) term is bigger, => votedFor = nil ", args.CandidateId)
		rf.currentTerm = args.Term
		rf.votedFor = nil
		rf.changeStateAndReinitialize(FOLLOWER)
	}

	// RequestVote RPC->Receiver implementation->2: If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	voteAvailable := rf.votedFor == nil || *(rf.votedFor) == args.CandidateId
	atLeastUpToDate := args.LastLogTerm > rf.getLastLogTerm() ||
		args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex()
	DebugLog(dVote, rf, "his{LastLogTerm=%v LastLogIndex=%v}, my{LastLogTerm=%v LastLogIndex=%v}", args.LastLogTerm, args.LastLogIndex, rf.getLastLogTerm(), rf.getLastLogIndex())
	DebugLog(dVote, rf, "voteAvailable=%v, atLeastUpToDate=%v", voteAvailable, atLeastUpToDate)
	if voteAvailable && atLeastUpToDate {
		DebugLog(dVote, rf, "Voted to S%v", args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = &args.CandidateId
	}

	if reply.VoteGranted {
		rf.resetElectionTicker()
	}
	DebugLog(dVote, rf, "my role=%v, handled reqVote from %v grant= %v", string(rf.state), args.CandidateId, reply.VoteGranted)

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) generateRequestVoteArgs() RequestVoteArgs {
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(), // index of candidate’s last log entry
		LastLogTerm:  rf.getLastLogTerm(),
	}
	return args
}

func (rf *Raft) electMyselfToBeLeader() {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	defer rf.resetElectionTicker()
	DebugLog(dVote, rf, "election timeout! start election!")
	// leader cannot start election
	if rf.state == FOLLOWER || rf.state == CANDIDATE {
		defer rf.persist()
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
		eventId := generateEventId(rf)
		// sendRequestVote to all peers in parallel
		DebugLog(dVote, rf, "sendRequestVote to all peers")
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(peerId int) { // pass the copied value
				rf.mutex.Lock()
				rf.persist() //persist before the server issues the next RPC
				rf.mutex.Unlock()
				reply := RequestVoteReply{}
				if rf.sendRequestVote(peerId, &args, &reply) {
					rf.mutex.Lock()
					defer rf.mutex.Unlock()
					defer rf.persist()
					if electionDone {
						return // late reply or peer server down
					}
					//response contains term T > currentTerm: set currentTerm = T, convert to follower
					if reply.Term > rf.currentTerm {
						rf.changeStateAndReinitialize(FOLLOWER)
						rf.currentTerm = reply.Term
					}

					// check everything remains the same
					if args.Term == rf.currentTerm &&
						args.LastLogIndex == rf.getLastLogIndex() &&
						args.LastLogTerm == rf.getLastLogTerm() &&
						rf.state == CANDIDATE /*still electing*/ {
						if reply.VoteGranted {
							voteCount++
						}
					}
					DebugLog(dVote, rf, "eid=%v receive vote from%v, curr vote=%v/%v", eventId, peerId, voteCount, len(rf.peers))

					if float64(voteCount)/float64(len(rf.peers)) > 0.5 {
						DebugLog(dVote, rf, "eid=%v, it is a leader now! tick heartbeat! ", eventId)
						electionDone = true
						rf.changeStateAndReinitialize(LEADER)
					}
				}
			}(i)
		}
	} else {
		DebugLog(dVote, rf, "election cannot start, role=%v", string(rf.state))
	}

}

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

type AppendEntriesReply struct {
	Term    int
	Success bool
	/*  optional
	when rejecting an AppendEntries request, the follower
	can include the term of the conflicting entry and
	the first index it stores for that term.
	*/
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
			DebugLog(dInfo, rf, "notify myself apply logs")
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
	if !(rf.getLastLogIndex() >= args.PrevLogIndex && /*contains contain an entry at prevLogIndex*/
		rf.logs[rf.logIdx2physicalIdx(args.PrevLogIndex)].Term == args.PrevLogTerm /*entry's index matches prevLogTerm*/) {
		reply.Success = false
		DebugLog(dClient, rf, "eid=%v, leaderId=%v, log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm. logs=%v", args.EventId, args.LeaderId, rf.logs2str(rf.logs))
		return
	}
	// 4. Append any new entries not already in the log
	for i, newLog := range args.Entries {
		newIndex := args.PrevLogIndex + i + 1 // the index of this new log
		newTerm := newLog.Term
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
	DebugLog(dClient, rf, "eid=%v, handle AppendEntries from %v Success= %v, refresh TO, CmitIdx=%v, appliedIdx=%v, log= %v", args.EventId, args.LeaderId, reply.Success, rf.commitIndex, rf.lastApplied, rf.logs2str(rf.logs))

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

		// since it sent heartbeat, just return and do not update anything,
		if len(args.Entries) == 0 {
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

// applyCommittedLogs
//
//	@Description: A gorountine that apply committed logs. It will wait until appliedIdx<committedIdx
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
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: nextIdx - 1,                                    // index of log entry immediately preceding new ones
					PrevLogTerm:  rf.logs[rf.logIdx2physicalIdx(nextIdx-1)].Term, // term of PrevLogIndex Log
					Entries:      rf.logs[rf.logIdx2physicalIdx(nextIdx):],
					LeaderCommit: rf.commitIndex,
					EventId:      eid, // only for debug
				}
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
		panic("why leader sends me a smaller snapshot?")
	}

	/*
	 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	 6. If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	 7. Discard the entire log
	*/
	//update rf.logs
	if rf.getLastLogIndex() <= args.LastIncludedIndex { //discard all logs: the snapshot covers all its logs (and its current snapshot, if it has one)
		rf.logs = make([]Log, 1)
	} else { // discard some of the logs
		newFirstLogIdx := args.LastIncludedIndex + 1
		newLogs := make([]Log, 1)
		newLogs = append(newLogs, rf.logs[rf.logIdx2physicalIdx(newFirstLogIdx):]...)
		rf.logs = newLogs
	}
	rf.snapshot = args.Data
	rf.snapshotLastIncludedIdx = args.LastIncludedIndex
	rf.snapshotLastIncludedTerm = args.LastIncludedTerm

	rf.firstLogIdx = args.LastIncludedIndex + 1
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	DebugLog(dSnap, rf, "handle InstallSnapshot from%v, splastIcuIdx=%v, splastIcuTerm=%v, firstIdx=%v, committedIdx=%v, appliedIdx=%v",
		args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm, rf.firstLogIdx, rf.commitIndex, rf.lastApplied)

	// 8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
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
		return index - rf.firstLogIdx + 1
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Task: trim logs to logs[index+1:], update firstLogIdx, and snapshotLastIncluded{Idx/Term}
	rf.mutex.Lock()
	defer rf.mutex.Unlock()

	if rf.firstLogIdx <= index /*assure this log index could be found in rf.logs*/ {
		DebugLog(dSnap, rf, "snapshot called!! idx=%v, s", index)
		// snapshot parameters
		newFirstLogIdx := index + 1
		lastIncludedIdx := index
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
		rf.persist()
	} else {
		DebugLog(dSnap, rf, "snapshot: invalid index")
	}

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

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
	//// immediately sent AppendEntries to followers (It's a better choice don't do this, which may overload the machine under high concurrent Start() request)
	//rf.heartbeatTicker.Reset(HEARTBEATTIMEOUT)
	//go rf.sendAppendEntriesToAllPeers()
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
	rf.electionTimeoutTicker.Stop()
	rf.heartbeatTimeoutTicker.Stop()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
		heartbeatTimeoutTicker:   time.NewTicker(HEARTBEATTIMEOUT),
		applyChan:                applyCh,
		firstLogIdx:              1,
		snapshot:                 nil,
		snapshotLastIncludedTerm: -1,
		snapshotLastIncludedIdx:  -1,
	}
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
