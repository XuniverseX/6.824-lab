package raft

type Log struct {
	Term    int
	Command interface{}
	Index   int
}

func (l *Log) Equals(log Log) bool {
	// Term与Index一致
	if l.Term == log.Term && l.Index == log.Index {
		return true
	}
	return false
}

func (rf *Raft) lastLog() Log {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) truncateLog(idx int) {
	rf.logs = rf.logs[:idx]
}

func (rf *Raft) leaderResetLog() {
	lastLog := rf.lastLog()
	for peer := range rf.nextIndex {
		rf.nextIndex[peer] = lastLog.Index + 1
		rf.matchIndex[peer] = 0
	}
}
