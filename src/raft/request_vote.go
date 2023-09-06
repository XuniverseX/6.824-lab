package raft

import "sync"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last logs entry (§5.4)
	LastLogTerm  int // term of candidate’s last logs entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) candidateSend(server int, args *RequestVoteArgs, count *int, once *sync.Once) {
	//DPrintf("[server %d]: term %v send RequestVote to %d\n", rf.me, args.Term, server)
	var reply RequestVoteReply
	ok := rf.sendRequestVote(server, args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > args.Term {
		DPrintf("[server %d] %d 在新的term，更新term，结束\n", rf.me, server)
		rf.convertToFollower(reply.Term)
		return
	}
	if reply.Term < args.Term {
		DPrintf("[server %d] %d 的term%d 已经失效，结束\n", rf.me, server, reply.Term)
		return
	}
	if !reply.VoteGranted {
		//DPrintf("[server %d] %d 没有投给me，结束\n", rf.me, server)
		return
	}

	*count++

	// rules Candidate 2
	if *count > len(rf.peers)/2 && rf.currentTerm == args.Term && rf.state == Candidate {
		DPrintf("[server %d] 获得多数选票，可以提前结束", rf.me)
		once.Do(func() {
			rf.state = Leader
			rf.leaderResetLog()
			// init
			//DPrintf("[server %d] leader - nextIndex %#v", rf.me, rf.nextIndex)
			rf.appendEntries(true)
		})
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	// rules RPC 1
	if args.Term < rf.currentTerm {
		//reply.VoteGranted = false // reply false
		return
	}

	// rules All Servers 2
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	// rules RPC 2
	lastLog := rf.lastLog()
	// 5.4.1
	upToDate := args.LastLogTerm > lastLog.Term ||
		(args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElectionTimeout()
		DPrintf("[server %v] term %v vote %v", rf.me, rf.currentTerm, rf.votedFor)
	}
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
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
