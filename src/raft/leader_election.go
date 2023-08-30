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
	rf.resetElectionTimeout()

	DPrintf("[server %d] term:%d leaderElection", rf.me, rf.currentTerm)

	lastLog := rf.lastLog()

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