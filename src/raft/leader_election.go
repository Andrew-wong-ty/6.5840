package raft

/****************************************************  RequestVote  *******************************************************/

// RequestVoteArgs the RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply the RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote the RequestVote RPC handler (receiver's handler)
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
