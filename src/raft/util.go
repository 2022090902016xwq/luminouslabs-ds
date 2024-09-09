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
	HeartbeatTimeout = 70
	ElectionTimeout  = 300
)

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond

}
func RandomizedElectionTimeout() time.Duration {
	t := ElectionTimeout + rand.Intn(ElectionTimeout)
	//Debug(dTimer, "RandomizedElectionTimeout t:%v", t)
	return time.Duration(t) * time.Millisecond
}

func (rf *Raft) genRequestVoteArgs() RequestVoteArgs {
	return RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}
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
		rf.electionTimer.Stop()
		rf.state = Leader
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.getLastIndex() + 1
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	case Follower:
		rf.state = Follower
		Debug(dTimer, "S%d RandomizedElectionTimeout", rf.me)
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	case Candidate:
		rf.state = Candidate
		rf.currentTerm += 1
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	}
}
func (rf *Raft) getTerm(curIndex int) int {
	Debug(dTerm, "S%d getTerm curIndex:%d, rf.lastIncludeIndex:%d", rf.me, curIndex, rf.lastIncludeIndex)
	// 如果当前index与快照一致/日志为空，直接返回快照/快照初始化信息，否则根据快照计算
	if curIndex-rf.lastIncludeIndex == 0 {
		return rf.lastIncludeTerm
	}
	return rf.log[curIndex-rf.lastIncludeIndex].Term
}

// 传入日志索引，返回索引对应日志的内容
func (rf *Raft) getLog(logIndex int) LogEntry {
	realIndex := logIndex - rf.lastIncludeIndex //切片中的索引
	//Debug(dLog, "S%d logIndex:%d, lastIncludeIndex:%d ,realIndex:%d, ", rf.me, logIndex, rf.lastIncludeIndex, realIndex)
	if realIndex >= len(rf.log) {
		Debug(dWarn, "S%d logIndex:%d, lastIncludeIndex:%d ,realIndex:%d, ", rf.me, logIndex, rf.lastIncludeIndex, realIndex)
	}
	return rf.log[realIndex]
}

func (rf *Raft) getLastIndex() int {
	if len(rf.log)-1 == 0 {
		return rf.lastIncludeIndex
	} else {
		return rf.lastIncludeIndex + len(rf.log) - 1
	}
}
func (rf *Raft) getLastTerm() int {
	if len(rf.log)-1 == 0 {
		return rf.lastIncludeTerm
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

// 将日志修剪，保存index之后的条目
func (rf *Raft) shrinkLog(index int) {
	//用新的切片切断原先的引用
	slog := make([]LogEntry, 0)
	slog = append(slog, LogEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		slog = append(slog, rf.getLog(i))
	}
	//修改参数
	if index == rf.getLastIndex()+1 {
		rf.lastIncludeTerm = rf.getLastTerm()
	} else {
		rf.lastIncludeTerm = rf.getTerm(index)
	}

	Debug(dSnap, "S%d shrinkLog, rf.getLog(index):%v", rf.me, rf.getLog(index))
	rf.lastIncludeIndex = index
	rf.log = slog
	Debug(dSnap, "S%d shrinkLog,rf.lastIncludeTerm:%d", rf.me, rf.lastIncludeTerm)
}
