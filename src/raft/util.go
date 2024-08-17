package raft

import (
	"math/rand"
	"time"
)

// Debugging
// const Debug = false

//	func DPrintf(format string, a ...interface{}) (n int, err error) {
//		if Debug {
//			log.Printf(format, a...)
//		}
//		return
//	}

const (
	HeartbeatTimeout = 75
	ElectionTimeout  = 150
)

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond

}
func RandomizedElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+rand.Intn(ElectionTimeout)) * time.Millisecond
}

func (rf *Raft) genRequestVoteArgs() RequestVoteArgs {
	return RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}
}
func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1
}
func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// 比较candidate日志是否比voter更新，termC和indexC对应candidate最后一个日志条目的任期和索引
// 如果两个日志的最后一个条目的任期不同，那么任期大的更新；
// 如果两个日志的最后一个条目的任期相同，日志更长的更新。
func (rf *Raft) isLogUpToDate(termC, indexC int) bool {
	indexRf := rf.getLastIndex() // rf最后一个日志条目的索引
	termRf := rf.getLastTerm()
	return (termC > termRf) || (termC == termRf && indexC >= indexRf)
}

// 封装状态转换的操作，改变身份为Follower用的比较多
func (rf *Raft) changeState(toState State) {
	switch toState {
	case Leader:
		rf.state = Leader
		rf.votedFor = -1
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	case Follower:
		rf.state = Follower
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	case Candidate:
		rf.state = Candidate
		rf.currentTerm += 1
	}
}
