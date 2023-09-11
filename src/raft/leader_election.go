package raft

import (
	"math/rand"
	"sync"
	"time"
)

func (rf *Raft) resetElectionTimeout() {
	electionTime := time.Duration(500+rand.Intn(150)) * time.Millisecond
	now := time.Now()
	rf.electionTime = now.Add(electionTime)
}

func (rf *Raft) leaderElection() {
	rf.state = Candidate
	// rules Candidate 1
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimeout()

	lastLog := rf.lastLog()

	DPrintf("[server %v]: start leader election, term %d\n", rf.me, rf.currentTerm)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	count := 1
	var once sync.Once
	for peer := range rf.peers {
		if peer != rf.me {
			go rf.candidateSend(peer, &args, &count, &once)
		}
	}
}
