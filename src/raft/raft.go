package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft server
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"luminouslabs-ds/labgob"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"luminouslabs-ds/labgob"
	"luminouslabs-ds/labrpc"
)

// as each Raft server becomes aware that successive log entries are
// committed, the server should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// 拓展：log压缩
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// LogEntry 日志条目
type LogEntry struct {
	Term    int         //任期
	Command interface{} //命令
}

// 服务器状态
type State int

const (
	Leader State = iota
	Candidate
	Follower
)

// A Go object implementing a single Raft server.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this server's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this server's persisted state
	me        int                 // this server's index into peers[]
	dead      int32               // set by Kill()

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persistent state on all peers
	currentTerm int        //当前任期
	votedFor    int        //投票给谁
	log         []LogEntry //日志，索引从1开始
	// persistent snapshot on all peers
	lastIncludeIndex int //快照索引
	lastIncludeTerm  int //快照任期
	// volatile state on all peers
	commitIndex int //已提交日志的最高索引，用于apply，初始为0
	lastApplied int //已应用到状态机的日志的最高索引，初始为0
	// volatile state on leaders
	nextIndex  []int //要发送的下一条目的索引，用于日志复制(initialized to leader last log index + 1)
	matchIndex []int //日志匹配的日志索引，用于日志复制和提交(initialized to 0, increases monotonically)
	// 自己添加的变量
	state          State // 服务器状态
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	applyChan      chan ApplyMsg //ApplyMsg通道，传递要apply到本地状态机的日志条目
	applyCond      *sync.Cond    // used to wakeup applier goroutine after committing new entries
	replicatorCond []*sync.Cond  // used to signal replicator goroutine to batch replicating entries
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	Debug(dInfo, "S%d, T%d, isLeader %v", rf.me, term, isleader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).

func (rf *Raft) persistData() []byte {
	// Your code here (拓展：持久化).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	return data
}
func (rf *Raft) persist() {
	raftState := rf.persistData()
	rf.persister.SaveRaftState(raftState)
	Debug(dPersist, "S%d, T%d, votedFor:%d, persist", rf.me, rf.currentTerm, rf.votedFor)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (拓展：持久化).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
	}
	// 同步快照信息
	if rf.lastIncludeIndex > 0 {
		rf.lastApplied = rf.lastIncludeIndex
	}
	Debug(dPersist, "S%d, T%d, votedFor:%d, lastIncludeIndex:%d, lastIncludeTerm:%d ,readPersist",
		rf.me, rf.currentTerm, rf.votedFor, rf.lastIncludeIndex, rf.lastIncludeTerm)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (拓展：日志压缩).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//check index
	if index <= rf.lastIncludeIndex || index > rf.commitIndex {
		Debug(dSnap, "S%d, T%d, index:%d, lastIncludeIndex:%d, no need to trim", rf.me, rf.currentTerm, index, rf.lastIncludeIndex)
		return
	}
	//shrink log
	rf.shrinkLog(index)
	rf.commitIndex = rf.lastIncludeIndex
	rf.lastApplied = rf.lastIncludeIndex
	//Debug(dTrace, "S%d snapshot:%v", rf.me, snapshot)
	//persist
	rf.persister.Save(rf.persistData(), snapshot)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) genInstallSnapshotArgs() *InstallSnapshotArgs {
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastIncludeIndex,
		LastIncludedTerm:  rf.lastIncludeTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	Debug(dSnap, "S%d genInstallSnapshotArgs to S%d", rf.me, args.LeaderID)
	return args
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	Debug(dSnap, "S%d send installSnapshot to S%d", rf.me, server)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//check Term
	if rf.currentTerm > args.Term {
		Debug(dSnap, "S%d find rf.currentTerm > args.Term (T:%d>%d)", rf.me, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		return
	} else {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}
	//check lastIncludeIndex
	if rf.lastIncludeIndex >= args.LastIncludedIndex {
		Debug(dSnap, "S%d find rf.lastIncludeIndex >= args.LastIncludedIndex (%d>=%d)", rf.me, rf.lastIncludeIndex, args.LastIncludedIndex)
		return
	}
	//heartbeat
	//rf.votedFor = -1
	rf.changeState(Follower)
	//trim log and set args
	index := args.LastIncludedIndex

	//用新的切片切断原先的引用
	slog := make([]LogEntry, 0)
	slog = append(slog, LogEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		slog = append(slog, rf.getLog(i))
	}
	//修改参数
	rf.lastIncludeTerm = args.LastIncludedTerm
	rf.lastIncludeIndex = index
	rf.log = slog

	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	Debug(dSnap, "S%d, T%d, lastIncludeIndex:%d, lastIncludeTerm:%d, commitIndex:%d, lastApplied:%d",
		rf.me, rf.currentTerm, rf.lastIncludeIndex, rf.lastIncludeTerm, rf.commitIndex, rf.lastApplied)
	//persist
	rf.persister.Save(rf.persistData(), args.Data)
	//apply snapshot
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	//Debug(dSnap, "S%d apply snapshot:%v", rf.me, msg)
	rf.applyChan <- msg
	//Debug(dTrace, "S%d snapshot:%v", rf.me, args.Data)
}

func (rf *Raft) handleInstallSnapshot(server int, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm { //Term check
		Debug(dSnap, "S%d, T%d, find rf.currentTerm > reply.Term (T:%d>%d)", rf.me, rf.currentTerm, rf.currentTerm, reply.Term)
		rf.changeState(Follower)
		rf.votedFor = -1
	} else { //change nextIndex
		rf.nextIndex[server] = rf.lastIncludeIndex + 1
	}

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int //candidate的任期
	CandidateId  int //candidate的编号
	LastLogIndex int //最新日志条目的编号
	LastLogTerm  int //最新日志条目的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  //Voter的当前任期，candidate用此比对任期决定是否更新
	VoteGranted bool //true表示投票给发RPC的candidate
}

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int        //leader的任期
	LeaderId     int        //leader的编号
	PrevLogIndex int        //前一个条目的索引
	PrevLogTerm  int        //前一个条目的任期
	Entries      []LogEntry //要存储的日志条目，空为心跳
	LeaderCommit int        //leader的commitIndex
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term          int  //follower的当前任期
	Success       bool //true表示follower包含匹配prevLogIndex和prevLogTerm的日志
	ConflictTerm  int  //the term of the conflict entry
	ConflictIndex int  //the index of the first entry with ConflictTerm
}

// follower检查选举超时和leader建立心跳机制
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			//选举超时，转变身份，任期++，开始选举,重启定时器
			rf.mu.Lock()
			Debug(dTerm, "S%d Converting to Candidate , calling election T:%d", rf.me, rf.currentTerm+1)
			rf.changeState(Candidate)
			rf.startElection()
			rf.electionTimer.Reset(RandomizedElectionTimeout()) //保证和其他节点的超时时间相近
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				Debug(dLeader, "S%d Sending Heartbeat", rf.me)
				rf.broadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}

	}
}

// 开始选举
func (rf *Raft) startElection() {
	//为自己投票，准备args，并行发送和处理RPC请求
	Debug(dVote, "S%d starts election, T%d", rf.me, rf.currentTerm)
	votesNum := 1
	rf.votedFor = rf.me
	rf.persist()
	args := rf.genRequestVoteArgs()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			res := rf.sendRequestVote(server, &args, &reply)
			if res { //处理返回结果
				rf.mu.Lock()
				defer rf.mu.Unlock()
				//检查任期、状态
				if rf.currentTerm < reply.Term {
					Debug(dTerm, "S%d Updates (T:%d->%d) and becomes Follower, S%d:T%d is higher", rf.me, rf.currentTerm, reply.Term, server, reply.Term)
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.changeState(Follower)
					rf.persist()
					return
				}
				//检查投票情况
				if reply.VoteGranted {
					votesNum++
					Debug(dVote, "S%d Got vote from S%d", rf.me, server)
					if rf.state == Candidate && votesNum > (len(rf.peers)-1)/2 {
						Debug(dLeader, "S%d Achieved Majority for T%d(votes %d), converting to Leader", rf.me, rf.currentTerm, votesNum)
						//成为Leader，关停选举超时定时器，重置心跳定时器，发个心跳看看实力
						rf.changeState(Leader)
						rf.broadcastHeartbeat(true)
					}
				}
			}
		}(i)

	}

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	Debug(dVote, "S%d Send RV to S%d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	//Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		Debug(dVote, "S%d Rejects vote request from S%d due to outdated term", rf.me, args.CandidateId)
		reply.VoteGranted = false
		return
	}
	//Server follows the Candidate(Term larger)
	if args.Term > rf.currentTerm {
		Debug(dTerm, "S%d Updates term(%d -> %d)", rf.me, rf.currentTerm, args.Term)
		//发现更大Term时只要重置follower身份，防止有更新日志的节点无法顺利发起选举
		rf.changeState(Follower)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	//If votedFor is null or candidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		rf.changeState(Follower)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		Debug(dVote, "S%d Grant vote to S%d", rf.me, args.CandidateId)
	} else {
		Debug(dVote, "S%d rejects vote request from S%d for votedFor||LogUptoDate", rf.me, args.CandidateId)
		Debug(dInfo, "S%d votedFor:%d, args.CandidateId:%d", rf.me, rf.votedFor, args.CandidateId)
		reply.VoteGranted = false
		return
	}
}

// 心跳维持领导力
func (rf *Raft) broadcastHeartbeat(isHeartBeat bool) {
	if rf.state != Leader {
		return
	}
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		if isHeartBeat {
			// need sending at once to maintain leadership
			Debug(dLeader, "S%d sends heartbeat to S%d", rf.me, server)
			go rf.replicateOneRound(server)
		} else {
			//just signal replicator goroutine to send entries in batch(client)
			rf.replicatorCond[server].Signal()
		}
	}
}

// leader向follower进行一轮复制
func (rf *Raft) replicateOneRound(server int) {
	rf.mu.RLock()
	// check status leader
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}
	Debug(dLeader, "S%d replicates logs/snapshot to S%d", rf.me, server)
	prevLogIndex := rf.nextIndex[server] - 1
	Debug(dSnap, "S%d nextIndex[%d]: %d, rf.lastIncludeIndex:%d, prevLogIndex[%d]:%d",
		rf.me, server, rf.nextIndex[server], rf.lastIncludeIndex, server, prevLogIndex)
	if rf.lastIncludeIndex > prevLogIndex {
		args := rf.genInstallSnapshotArgs()
		rf.mu.RUnlock()
		reply := new(InstallSnapshotReply)
		if rf.sendInstallSnapshot(server, args, reply) {
			go rf.handleInstallSnapshot(server, reply)
		}
	} else {
		// prepare AppendEntries args
		args := rf.genAppendEntriesArgs(server)
		rf.mu.RUnlock()
		reply := new(AppendEntriesReply)
		// send AppendEntries and handle AppendEntries reply
		if rf.sendAppendEntries(server, args, reply) {
			go rf.handleAppendEntriesReply(server, args, reply)
		}
	}

}

func (rf *Raft) genAppendEntriesArgs(server int) *AppendEntriesArgs {
	realNextIndex := rf.nextIndex[server] - rf.lastIncludeIndex
	Debug(dSnap, "S%d genAppendEntriesArgs realNextIndex: %d, rf.nextIndex[%d]: %d, rf.lastIncludeIndex: %d",
		rf.me, realNextIndex, server, rf.nextIndex[server], rf.lastIncludeIndex)
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.getTerm(rf.nextIndex[server] - 1),
		LeaderCommit: rf.commitIndex,
	}
	Debug(dLog, "S%d AE-Args to S%d LeaderCommit%d", rf.me, server, args.LeaderCommit)
	if rf.getLastIndex() >= rf.nextIndex[server] {
		entries := make([]LogEntry, 0)
		entries = append(entries, rf.log[realNextIndex:]...) //解引用
		args.Entries = entries
	} else {
		args.Entries = []LogEntry{}
	}
	//Debug(dLeader, "S%d Make AE-Args to S%d", rf.me, server)
	//Debug(dInfo, "S%d AE-Args to S%d-", rf.me, server, *args)
	return args
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	Debug(dLog, "S%d send AE to S%d", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//Term check: Reply false if term < currentTerm (§5.1), heartbeat not available
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = -1
		reply.ConflictTerm = 0
		Debug(dLog, "S%d rejects AE from S%d due to outdated term", rf.me, args.LeaderId)
		return
	}
	//HeartBeat response: corrective Term , reset status and timer
	rf.changeState(Follower)
	rf.currentTerm = args.Term
	Debug(dLog, "S%d accepts AE from S%d, leaderCommit%d vs followerCommit%d, args.PrevLogIndex:%d, rf.lastIncludeIndex:%d",
		rf.me, args.LeaderId, args.LeaderCommit, rf.commitIndex, args.PrevLogIndex, rf.lastIncludeIndex)
	//快照和日志的冲突
	if args.PrevLogIndex < rf.lastIncludeIndex {
		reply.Term, reply.Success = rf.currentTerm, false
		reply.ConflictIndex = rf.lastIncludeIndex + 1
		reply.ConflictTerm = -1
		Debug(dSnap, "S%d AE: need snapshot from S%d: prevLogIndex %d < lastIncludeIndex %d", rf.me, args.LeaderId, args.PrevLogIndex, rf.lastIncludeIndex)
		return
	}
	//Logs match and conflict: Reply false and conflict information if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if !rf.matchLog(rf.me, args.PrevLogIndex, args.PrevLogTerm) {
		Debug(dLog, "S%d AE: log conflict from S%d", rf.me, args.LeaderId)
		reply.Success, reply.Term = false, rf.currentTerm

		if rf.getLastIndex() < args.PrevLogIndex { //log shorter conflict
			Debug(dLog, "S%d AE: Log conflict from S%d for log length", rf.me, args.LeaderId)
			reply.ConflictTerm = -1
			reply.ConflictIndex = rf.getLastIndex() + 1
		} else { //log entry conflict, find conflict information
			reply.ConflictTerm = rf.getTerm(args.PrevLogIndex)
			//search ConflictIndex
			i := args.PrevLogIndex
			for i > rf.lastIncludeIndex+1 && rf.getTerm(i) == reply.ConflictTerm {
				i--
			}
			reply.ConflictIndex = i
			Debug(dLog, "S%d Log conflict from S%d for log entry, ConflictIndex:%d ConflictTerm%d",
				rf.me, args.LeaderId, reply.ConflictIndex, reply.ConflictTerm)
		}
		return
	}
	//Append any new entries not already in the log
	if len(args.Entries) > 0 {
		rf.log = append(rf.log[:args.PrevLogIndex-rf.lastIncludeIndex+1], args.Entries...)
		Debug(dLog, "S%d appends new entries from S%d, S%d-log-len(log):%d", rf.me, args.LeaderId, rf.me, len(rf.log))
		//Debug(dInfo, "S%d log-", rf.me, rf.log)
	}
	//set commitIndex and then signal applyCond
	rf.followerCommitLog(args.LeaderCommit)
	//reply update, no conflict so just reply success and term
	reply.Success, reply.Term = true, rf.currentTerm
	//Debug(dInfo, "S%d AE-Reply to S%d-", rf.me, args.LeaderId, *reply)
}

// check entry at prevLogIndex whose term matches prevLogTerm
func (rf *Raft) matchLog(server, prevLogIndex, PrevLogTerm int) bool {
	//check len
	if rf.getLastIndex() < prevLogIndex {
		return false
	}
	//check term of log with prevLogIndex
	if rf.getTerm(prevLogIndex) != PrevLogTerm {
		Debug(dLog, "S%d !matchLog from %d rf.getTerm(prevLogIndex):%d, PrevLogTerm:%d", rf.me, server, rf.getTerm(prevLogIndex), PrevLogTerm)
		return false
	}
	return true
}

func (rf *Raft) followerCommitLog(leaderCommit int) {
	Debug(dInfo, "S%d followerCommitLog:%d, leaderCommit:%d, rf.getLastIndex():%d, rf.lastApplied:%d",
		rf.me, rf.commitIndex, leaderCommit, rf.getLastIndex(), rf.lastApplied)
	if leaderCommit > rf.commitIndex {
		rf.commitIndex = min(leaderCommit, rf.getLastIndex())
		Debug(dInfo, "S%d followerCommitLog: commitIndex:%d", rf.me, rf.commitIndex)
	}
	if rf.lastApplied < rf.commitIndex {
		rf.applyCond.Signal()
		Debug(dApply, "S%d signal applyCond", rf.me)
	}
}
func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//check leader status and term
	if rf.state == Leader && rf.currentTerm == args.Term {
		//check reply.success
		if reply.Success { //log match, update nextIndex and matchIndex
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			Debug(dLog2, "S%d matches log from S%d, nextIndex[%d]:%d, matchIndex[%d]:%d, args.PrevLogIndex:%d, len(args.Entries):%d",
				rf.me, server, server, rf.nextIndex[server], server, rf.matchIndex[server], args.PrevLogIndex, len(args.Entries))
			rf.leaderCommitApplyLog()
		} else { //conflict
			Debug(dLog, "S%d leader finds log conflict from S%d", rf.me, server)
			Debug(dInfo, "S%d leader currentTerm:%d, S%d reply.Term:%d", rf.me, rf.currentTerm, server, reply.Term)
			//term conflict
			if reply.Term > rf.currentTerm {
				Debug(dTerm, "S%d HAE: Term outDate ,change status to Follower", rf.me)
				rf.changeState(Follower)
				rf.currentTerm, rf.votedFor = reply.Term, -1
				rf.persist()
			} else if reply.Term == rf.currentTerm {
				if reply.ConflictTerm != -1 { //log conflict for index and term
					Debug(dLog2, "S%d leader finds log conflict with S%d, ConflictIndex:%d, ConflictTerm:%d, nextIndex[%d]:%d",
						rf.me, server, reply.ConflictIndex, reply.ConflictTerm, server, rf.nextIndex[server])
					rf.nextIndex[server] = reply.ConflictIndex
					//search ConflictTerm
					for i := args.PrevLogIndex; i > reply.ConflictIndex; i-- {
						if rf.getTerm(i) == reply.ConflictTerm {
							rf.nextIndex[server] = i + 1
							break
						}
					}
					Debug(dLog2, "S%d after search, nextIndex[%d]: %d ", rf.me, server, rf.nextIndex[server])
				} else { //log conflict for log length
					rf.nextIndex[server] = reply.ConflictIndex
					Debug(dLog2, "S%d leader find log conflict with S%d for log length, rf.nextIndex[%d]:%d", rf.me, server, server, rf.nextIndex[server])
				}
				//日志冲突，立即发送一轮AppendEntries来同步日志;会不会和心跳冲突呢;有所问题
				go rf.replicateOneRound(server)
			}
		}
	}
}

func (rf *Raft) leaderCommitApplyLog() {
	//copy and sort matchIndex
	sortedMatchIndex := make([]int, len(rf.matchIndex))
	copy(sortedMatchIndex, rf.matchIndex)
	sort.Ints(sortedMatchIndex)
	//find N, a majority of matchIndex[i] ≥ N
	N := sortedMatchIndex[len(sortedMatchIndex)/2]
	//check N,N > commitIndex, and log(index==N).term == currentTerm:set commitIndex = N
	if N > rf.commitIndex && rf.getTerm(N) == rf.currentTerm {
		rf.commitIndex = N
		Debug(dCommit, "S%d commits up to index %d", rf.me, rf.commitIndex)
	}
	if rf.lastApplied < rf.commitIndex {
		rf.applyCond.Signal()
		Debug(dApply, "S%d(leader) signal applyCond", rf.me)
	}
}

// applier push entries in order to applyCh sync
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// if no need to apply, just wait in condition
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		Debug(dApply, "S%d-Ready to apply logs. commitIndex=%d, lastApplied=%d", rf.me, rf.commitIndex, rf.lastApplied)
		//if commitindex > lastApplied, apply log[lastApplied+1:commitIndex]
		commitIndex, lsatApplied := rf.commitIndex, rf.lastApplied
		realCommitIndex, realLastApplied := commitIndex-rf.lastIncludeIndex, lsatApplied-rf.lastIncludeIndex
		if rf.lastApplied == 0 {
			realLastApplied = 0
		}

		entries := make([]LogEntry, commitIndex-lsatApplied)
		copy(entries, rf.log[realLastApplied+1:realCommitIndex+1])
		Debug(dApply, "S%d-Applying logs from index %d to %d", rf.me, lsatApplied+1, commitIndex)
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyChan <- ApplyMsg{
				Command:      entry.Command,
				CommandIndex: lsatApplied + 1,
				CommandValid: true,
			}
			lsatApplied++
		}
		rf.mu.Lock()
		rf.lastApplied = max(lsatApplied, commitIndex)
		Debug(dApply, "S%d-Updated lastApplied to %d", rf.me, rf.lastApplied)
		rf.mu.Unlock()
	}
}

// 专门用来响应客户机的复制协程
func (rf *Raft) replicator(server int) {
	rf.replicatorCond[server].L.Lock()
	defer rf.replicatorCond[server].L.Unlock()
	for rf.killed() != false {
		for !rf.needReplicating(server) {
			rf.replicatorCond[server].Wait()
		}
		// maybe a pipeline mechanism is better to trade-off the memory usage and catch up time
		rf.replicateOneRound(server)
	}
}

// if matchIndex is small, need replicate one round
func (rf *Raft) needReplicating(server int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.matchIndex[server] < rf.getLastIndex()
}

// propose new log entries to the Raft cluster
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (拓展：日志复制).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	} else {
		//prepare new log entry
		index = rf.getLastIndex() + 1
		term = rf.currentTerm
		newEntry := LogEntry{
			Term:    term,
			Command: command,
		}
		//leader append , persist log, and update matchIndex for itself
		rf.log = append(rf.log, newEntry)
		rf.persist()
		rf.matchIndex[rf.me] = rf.getLastIndex()
		Debug(dClient, "S%d Append new entry to log-%v", rf.me, newEntry)
		Debug(dLog2, "S%d Updated matchIndex[%d]:%d, len(log):%d", rf.me, rf.me, rf.matchIndex[rf.me], len(rf.log))
		//启动复制
		rf.broadcastHeartbeat(false)
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft peers (including this one) are in peers[]. this
// server's port is peers[me]. all the peers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		dead:        0,
		currentTerm: 0,
		commitIndex: 0,
		lastApplied: 0,
		votedFor:    -1,
		state:       Follower,
		log:         make([]LogEntry, 1),
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),

		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
		replicatorCond: make([]*sync.Cond, len(peers)),
		applyChan:      applyCh,
	}
	Debug(dInfo, "S%d Make successful", rf.me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// Your initialization code here.
	rf.applyCond = sync.NewCond(&rf.mu)
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i], rf.matchIndex[i] = rf.getLastIndex()+1, 0
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(i)
		}
	}
	// start heartbeat or election
	go rf.ticker()
	// apply logs
	go rf.applier()

	return rf
}
