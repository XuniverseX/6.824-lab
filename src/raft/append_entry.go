package raft

type AppendEntriesArgs struct {
	Term         int   // leader’s term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of logs entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // logs entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	Inconsistency bool
}

// leader发送appendEntries
func (rf *Raft) appendEntries(heartbeat bool) {
	lastLog := rf.lastLog()
	// 遍历所有服务器的下标
	for peer := range rf.peers {
		//如果遍历到本leader服务器，重置选举超时时间
		if peer == rf.me {
			rf.resetElectionTimeout()
			continue
		}

		//DPrintf("[server %d] My lastLog is %d, server %d nextIndex is %d", rf.me, lastLog.Index, peer, rf.nextIndex[peer])

		// rules for leaders 3
		if lastLog.Index >= rf.nextIndex[peer] || heartbeat {
			nextIndex := rf.nextIndex[peer]
			if nextIndex <= 0 {
				nextIndex = 1
			}
			prevLog := rf.logs[nextIndex-1]
			//切片需要复制后传进参数
			cloneLogs := make([]Log, rf.lastLog().Index-nextIndex+1)
			copy(cloneLogs, rf.logs[nextIndex:])

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				Entries:      cloneLogs,
				LeaderCommit: rf.commitIndex,
			}
			go rf.leaderSend(peer, &args)
		}
	}
}

func (rf *Raft) leaderSend(server int, args *AppendEntriesArgs) {

	var reply AppendEntriesReply
	DPrintf("[server %d] Term:%d send AppendEntries to %d, leadercommit:%v", rf.me, rf.currentTerm, server, args.LeaderCommit)
	if !rf.sendAppendEntries(server, args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rules Leaders 3.1
	if reply.Success {
		// 更新nextIndex和matchIndex
		match := args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = match + 1
		rf.matchIndex[server] = match
	} else if reply.Inconsistency {
		// todo
	} else if rf.nextIndex[server] > 1 {
		rf.nextIndex[server]--
	}

	// rules Leader 4
	if rf.state != Leader {
		return
	}

	for n := rf.commitIndex + 1; n <= rf.lastLog().Index; n++ {
		if rf.logs[n].Term != rf.currentTerm {
			continue
		}
		count := 0
		for peer := range rf.peers {
			if rf.matchIndex[peer] >= n {
				count++
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = n
				rf.apply()
				break
			}
		}
	}

}

// AppendEntries 接收rpc请求接口
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//初始化reply
	reply.Success = false
	reply.Term = rf.currentTerm

	// rules All Servers 2
	if args.Term > rf.currentTerm {
		if rf.state != Follower {
			rf.state = Follower
		}
		rf.currentTerm = args.Term
	}

	// rules RPC 1
	if args.Term < rf.currentTerm {
		return
	}

	// rules Candidate 3
	if rf.state == Candidate {
		rf.state = Follower
	}

	rf.resetElectionTimeout() // 重置选举定时器
	//DPrintf("[server %d] resetTimout", rf.me)

	// rules RPC 2
	contains := false
	for _, entry := range rf.logs {
		if entry.Term == args.PrevLogTerm && entry.Index == args.PrevLogIndex {
			contains = true
		}
	}
	// 如果当前follower中没有日志与leader保存的prevLog相同，返回false
	if !contains {
		reply.Inconsistency = true
		return
	}

	// rules RPC 3, 4
	for idx, entry := range args.Entries {
		if entry.Index <= rf.lastLog().Index && entry.Term != rf.logs[entry.Index].Term {
			rf.truncateLog(entry.Index)
		}
		//添加不存在的新日志
		if entry.Index > rf.lastLog().Index {
			rf.logs = append(rf.logs, args.Entries[idx:]...)
		}
	}

	// rules RPC 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLog().Index)
		DPrintf("in")
		rf.apply()
	}

	//DPrintf("[server %d] commitIndex: %d, LeaderCommit: %d", rf.me, rf.commitIndex, args.LeaderCommit)

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
