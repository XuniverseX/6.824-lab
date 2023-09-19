module kvraft

go 1.19

replace (
	raft => ../raft
	porcupine => ../porcupine
	models => ../models
	labrpc => ../labrpc
	labgob => ../labgob
)

require (
	raft v0.0.0
	porcupine v0.0.0
	models v0.0.0
	labrpc v0.0.0
	labgob v0.0.0
)