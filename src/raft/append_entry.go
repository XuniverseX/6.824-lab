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
func (rf *Raft) appendEntries() {
	// 遍历所有服务器的下标
	for peer := range rf.peers {
		//如果遍历到本leader服务器，重置选举超时时间
		if peer == rf.me {
			rf.resetElectionTimeout()
			continue
		}

		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		lastLog := rf.lastLog()

		// rules for leaders 3
		if lastLog.Index >= rf.nextIndex[peer] {
			nextIndex := rf.nextIndex[peer]
			if nextIndex <= 0 {
				nextIndex = 1
			}
			prevLog := rf.logs[nextIndex-1]
			//切片需要复制后传进参数
			cloneLogs := make([]Log, rf.lastLog().Index-nextIndex+1)
			copy(cloneLogs, rf.logs[nextIndex:])

			args.PrevLogIndex = prevLog.Index
			args.PrevLogTerm = prevLog.Term
			args.Entries = cloneLogs
			args.LeaderCommit = rf.commitIndex
		}
		go rf.leaderSend(peer, &args)
	}
}

func (rf *Raft) leaderSend(server int, args *AppendEntriesArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var reply AppendEntriesReply
	DPrintf("[server %d] Term:%d send AppendEntries to %d", rf.me, rf.currentTerm, server)
	if !rf.sendAppendEntries(server, args, &reply) {
		return
	}

	// rules for leaders 3.1
	if reply.Success {
		// 更新nextIndex和matchIndex
		rf.nextIndex[server] = args.PrevLogIndex + 1
		rf.matchIndex[server] = args.PrevLogIndex
		return
	}
	if !reply.Inconsistency {
		return
	}
	// rules for leaders 3.2
	rf.nextIndex[server]--

	rf.mu.Unlock()
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
		rf.currentTerm = args.Term
		if rf.state != Follower {
			rf.state = Follower
		}
		//return
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
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
