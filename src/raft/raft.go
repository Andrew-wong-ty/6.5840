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
	Command interface{}
	Term    int // first index is 1
}

type LeaderState struct {
	nextIndex  []int
	matchIndex []int
}

type Role string

const (
	FOLLOWER  Role = "foll"
	CANDIDATE      = "candi"
	LEADER         = "lead"
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

	state Role

	//! for leader election,
	electionTimeoutTicker *time.Ticker

	//! for managing heartbeat
	heartbeatTimeoutTicker *time.Ticker

	// for notifying apply
	applyCond *sync.Cond
	applyChan chan ApplyMsg
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
		case LEADER:
			rf.state = LEADER
			// stop election ticker
			rf.electionTimeoutTicker.Stop()
			// reinitialized matchIndex[] and nextIndex[]
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.getLastLogIndex() + 1
				rf.matchIndex[i] = 0
			}
			// immediately trigger a heartbeat
			rf.heartbeatTimeoutTicker.Reset(HEARTBEATTIMEOUT)
			go rf.sendAppendEntriesToAllPeers()
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

// index of candidate’s last log entry
func (rf *Raft) getLastLogIndex() int {
	// len(logs)-1 since its first element's index is 1
	return len(rf.logs) - 1
}

// term of candidate’s last log entry
func (rf *Raft) getLastLogTerm() int {
	if len(rf.logs) == 1 {
		// empty logs
		return 0 // when initializing, all servers are followers and term is 0
	} else {
		return rf.logs[len(rf.logs)-1].Term
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
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
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
	var logs []Log
	err := d.Decode(&currentTerm)
	if err != nil {
		return
	}
	err = d.Decode(&votedForVal)
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
	rf.logs = logs
	DebugLog(dPersist, rf, "readPersist success, T=%v, voteFor=%v, logs=%v", currentTerm, votedForVal, logs2str(logs))
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	defer rf.persist() //persisted before the server replies to an ongoing RPC.
	defer DebugLog(dVote, rf, "my role=%v, handled reqVote from %v grant= %v", string(rf.state), args.CandidateId, reply.VoteGranted)

	reply.Term = max(rf.currentTerm, args.Term)
	reply.VoteGranted = false
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
	voteAvailable := rf.votedFor == nil || *(rf.votedFor) == args.CandidateId
	atLeastUpToDate := args.LastLogTerm > rf.getLastLogTerm() ||
		args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex()
	if voteAvailable && atLeastUpToDate {
		reply.VoteGranted = true
		rf.votedFor = &args.CandidateId
	}
	if reply.VoteGranted {
		rf.resetElectionTicker()
	}

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
		DebugLog(dVote, rf, "election can start")
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
			go func(peerId int) {
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
					if args.Term == rf.currentTerm && // everything remains the same
						args.LastLogIndex == rf.getLastLogIndex() &&
						args.LastLogTerm == rf.getLastLogTerm() &&
						rf.state == CANDIDATE /*still electing*/ {
						if reply.VoteGranted {
							voteCount++
						}
					}
					DebugLog(dVote, rf, "eid=%v vote=%v/%v", eventId, voteCount, len(rf.peers))

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
	EventId      string // for debug
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
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	defer rf.persist() //persist before the server replies to an ongoing RPC.
	defer rf.resetElectionTicker()
	// Rule.All Servers.1
	defer func() {
		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Signal() // notify apply logs
		}
	}()
	// fill XMessage when fail to match rf.getLastLog{Index/Term}
	defer func() {
		reply.XLen, reply.XIndex, reply.XTerm = -1, -1, -1
		if reply.Success == false {
			reply.XLen = len(rf.logs)
			if rf.getLastLogIndex() >= args.PrevLogIndex {
				if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm { // it has the index but with conflicting term
					reply.XTerm = rf.logs[args.PrevLogIndex].Term
					// find the first index with XTerm
					reply.XIndex = -1
					for idx, logItem := range rf.logs {
						if logItem.Term == reply.XTerm {
							reply.XIndex = idx
							break
						}
					}
					if reply.XIndex == -1 {
						panic("unexpected: reply.XIndex == -1")
					}
				}
			}
		}
	}()

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
		DebugLog(dClient, rf, "eid=%v, receive HB with outdated term from %v; reject", args.EventId, args.LeaderId)
		return
	}
	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if !(rf.getLastLogIndex() >= args.PrevLogIndex && /*contains contain an entry at prevLogIndex*/
		rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm /*entry's index matches prevLogTerm*/) {
		reply.Success = false
		DebugLog(dClient, rf, "eid=%v, leaderId=%v, log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm. mylogs=%v", args.EventId, args.LeaderId, logs2str(rf.logs))
		return
	}
	// 4. Append any new entries not already in the log
	for i, newLog := range args.Entries {
		newIndex := args.PrevLogIndex + i + 1 // the index of this new log
		newTerm := newLog.Term
		// 3. check if an existing entry conflicts with a new one (same index but different terms)
		if rf.getLastLogIndex() >= newIndex && rf.logs[newIndex].Term != newTerm {
			// delete the existing entry and all that follow it
			rf.logs = rf.logs[:newIndex]
		}
		if !(rf.getLastLogIndex() >= newIndex && /*check if "newLog" already exist*/
			rf.logs[newIndex].Term == newLog.Term && rf.logs[newIndex].Command == newLog.Command) {
			rf.logs = append(rf.logs, newLog)
		}
		if rf.logs[newIndex].Term != newTerm {
			panic("rf.logs[newIndex].Term!=newTerm after append!!!????")
		}
	}
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
	}
	DebugLog(dClient, rf, "eid=%v, handle AppendEntries from %v Success= %v, refresh TO, CmitIdx=%v, AppedIdx=%v, log= %v", args.EventId, args.LeaderId, reply.Success, rf.commitIndex, rf.lastApplied, logs2str(rf.logs))

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
	return hasXTerm, lastEntryIdxWithXTerm
}

// sendAppendEntriesToOnePeer
//
//	@Description: given AppendEntriesArgs, send heartbeat to a follower. Upon reply, update nextIndex and matchIndex
//	@receiver rf
//	@param peerId: the server you're sending to
//	@param logStartIndex: you want to send rf.logs[logStartIndex:] to the peer
func (rf *Raft) sendAppendEntriesToOnePeer(followerId, nextIdx int, args AppendEntriesArgs, eventId /*for debug*/ string) {
	if rf.killed() {
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

		// since it sent heartbeat, just return and do not update anything
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
				hasXTerm, lastEntryIdxWithXTerm := rf.findConflictTerm(reply.XTerm)
				if hasXTerm {
					rf.nextIndex[followerId] = max(1, lastEntryIdxWithXTerm)
				} else {
					rf.nextIndex[followerId] = max(1, reply.XIndex)
				}
				DebugLog(dLeader, rf, "eid=%v, (check fail) follower=%v  hasXTerm=%v, lastEntryIdxWithXTerm=%v", eventId, followerId, hasXTerm, lastEntryIdxWithXTerm)
			} else {
				// follower too short!
				DebugLog(dLeader, rf, "eid=%v, (check fail) follower=%v too short! Xlen=%v", eventId, followerId, reply.XLen)

				rf.nextIndex[followerId] = max(1, reply.XLen)
			}
			// the leader decrements nextIndex and retries the AppendEntries RPC.
			//rf.nextIndex[followerId] = max(1, rf.nextIndex[followerId]-1)
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
			if float64(count)/float64(len(rf.peers)) > 0.5 /*majority committed*/ {
				if rf.logs[N].Term == rf.currentTerm {
					DebugLog(dLeader, rf, "eid=%v, majority received! update leader commitIndex =%v", eventId, N)
					rf.commitIndex = N
				}
			}
		}
	}
}

func (rf *Raft) applyCommittedLogs() {
	for rf.killed() == false {
		rf.mutex.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait() // wait until appliedIdx<committedIdx; and acquire lock again
		}
		index := rf.lastApplied + 1
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[index].Command,
			CommandIndex: index,
		}
		DebugLog(dCommit, rf, "committed {Idx=%v, T=%v, Cmd=%v}, lastApp=%v, cmitIdx=%v",
			index, rf.logs[index].Term, rf.logs[index].Command, rf.lastApplied, rf.commitIndex)
		rf.lastApplied++
		rf.mutex.Unlock()
		rf.applyChan <- msg // To prevent possible deadlock, put this outside the lock context
	}
}

func (rf *Raft) sendAppendEntriesToAllPeers() {
	if rf.killed() {
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
			if leaderLastLogIdx >= nextIdx {
				// need to send new logs (rf.logs[nextIdx:]) to followers
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: nextIdx - 1,             // index of log entry immediately preceding new ones
					PrevLogTerm:  rf.logs[nextIdx-1].Term, // term of PrevLogIndex Log
					Entries:      rf.logs[nextIdx:],
					LeaderCommit: rf.commitIndex,
					EventId:      eid,
				}
				DebugLog(dLeader, rf, "sent AppendEntries to %v, eid=%v, cmitIdx=%v, appedIdx=%v, leader lastLogIdx=%v, nextIndex[%v]=%v, new log len=%v, currLogs= %v", followerId, eid, rf.commitIndex, rf.lastApplied, leaderLastLogIdx, followerId, nextIdx, len(args.Entries), logs2str(rf.logs))
				// send to append entries
				go rf.sendAppendEntriesToOnePeer(followerId, nextIdx, args, eid)
			} else {
				// send empty logs
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.getLastLogIndex(), // last log index
					PrevLogTerm:  rf.getLastLogTerm(),  // term of last log
					Entries:      make([]Log, 0),       // empty logs
					LeaderCommit: rf.commitIndex,
					EventId:      eid,
				}
				DebugLog(dLeader, rf, "sent HB to %v, eid=%v,  cmitIdx=%v, appedIdx=%v, leader lastLogIdx=%v, nextIndex[%v]=%v, currLogs= %v", followerId, eid, rf.commitIndex, rf.lastApplied, leaderLastLogIdx, followerId, nextIdx, logs2str(rf.logs))
				// send for heartbeat
				go rf.sendAppendEntriesToOnePeer(followerId, nextIdx, args, eid)
			}
		}
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
		mutex:                  sync.Mutex{},
		peers:                  peers,
		persister:              persister,
		me:                     me,
		dead:                   0,
		currentTerm:            0,
		votedFor:               nil,
		logs:                   make([]Log, 1), // since log starts with index 1, the first element is a dummy one
		commitIndex:            0,
		lastApplied:            0,
		nextIndex:              make([]int, len(peers)),
		matchIndex:             make([]int, len(peers)),
		state:                  FOLLOWER,
		electionTimeoutTicker:  time.NewTicker(randomElectionDuration()),
		heartbeatTimeoutTicker: time.NewTicker(HEARTBEATTIMEOUT),
		applyChan:              applyCh,
	}
	rf.applyCond = sync.NewCond(&rf.mutex)
	rf.heartbeatTimeoutTicker.Stop() // wait until being a leader
	rf.electionTimeoutTicker.Stop()

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections  / send heartbeat
	go rf.ticker()
	// start the goroutine to commit logs
	go rf.applyCommittedLogs()
	//go rf.sendHeartbeats()
	DebugLog(dInfo, rf, "Raft server is made, nPeers=%v", len(peers))
	// start election ticker
	rf.electionTimeoutTicker.Reset(randomElectionDuration())
	return rf
}
