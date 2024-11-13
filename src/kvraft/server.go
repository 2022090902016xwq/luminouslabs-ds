package kvraft

import (
	"log"
	"luminouslabs-ds/labgob"
	"luminouslabs-ds/labrpc"
	"luminouslabs-ds/raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key    string
	Value  string

	ClientId int64
	SeqId    int
	//Index    int // raft服务层传来的日志索引
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	seqMap    map[int64]int     //为了确保seq只执行一次 clientId / seqId
	waitChMap map[int]chan Op   //传递由下层Raft服务的appCh传过来的command index / chan(Op)
	kvPersist map[string]string // 存储持久化的KV键值对 K / V

	lastApplied      int
	lastIncludeIndex int
	persister        *raft.Persister
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.seqMap = make(map[int64]int)
	kv.waitChMap = make(map[int]chan Op)
	kv.kvPersist = make(map[string]string)

	//kv.lastIncludeIndex = -1 ?
	//kv.decodeSnapshot(kv.rf.GetLastApplied(), kv.rf.GetSnapshot())
	kv.decodeSnapshot(kv.rf.GetLastIncludeIndex(), kv.persister.ReadSnapshot())
	go kv.applyMsgHandlerLoop()
	return kv
}

// Get 主要流程
// - 检查当前服务器是否存活、是否为 Leader。如果不是，则返回 `ErrWrongLeader` 错误。
// - 封装一个 `Op` 操作并通过 Raft 的 `Start` 方法发送。
// - 利用 `waitChMap` 管理的通道等待 `applyCh` 的返回值，处理超时和成功响应，在函数结束后删除通道。
// - 若操作成功，从 `kvPersist` 中获取对应键的值并返回。
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	Op := Op{OpType: "Get", Key: args.Key, ClientId: args.ClientId, SeqId: args.SeqId}
	lastIndex, _, _ := kv.rf.Start(Op)

	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, lastIndex)
		kv.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	//等待
	select {
	case replyOp := <-ch:
		//请求错误
		if replyOp.ClientId != args.ClientId || replyOp.SeqId != args.SeqId {
			reply.Err = ErrWrongLeader
			//return
		} else { //请求正确
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvPersist[args.Key] // linearized
			kv.mu.Unlock()
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

// PutAppend 流程
// - 检查当前服务器是否存活、是否为 Leader。如果不是，则返回 `ErrWrongLeader` 错误。
// - 创建 `Op` 操作并通过 Raft 的 `Start` 方法发送。
// - 使用 `waitChMap` 里的通道等待操作应用，处理超时与成功情况。
// - 操作成功则返回 `OK`，否则返回 `ErrWrongLeader`。
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	Op := Op{OpType: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, SeqId: args.SeqId}
	lastIndex, _, _ := kv.rf.Start(Op)

	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, lastIndex)
		kv.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if replyOp.ClientId != args.ClientId || replyOp.SeqId != args.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

// applyMsgHandlerLoop
// - **作用**：循环处理 Raft 的 `applyCh` 中的消息，应用到 `KVServer` 的状态机中。
// - **逻辑**：
//   - 从 `applyCh` 获取 `ApplyMsg` 消息，提取出 `Op` 操作和索引。
//   - 检查该操作是否已被执行（防止重复），如果是 `Put` 或 `Append` 操作，则更新 `kvPersist` 中的键值对。对Get操作忽略，交由Get方法自行去kvPersist中获取
//   - 更新 `seqMap` 以记录最新操作序列。
//   - 将操作通过 `waitChMap` 中的通道返回给相应的请求 Goroutine。
func (kv *KVServer) applyMsgHandlerLoop() {
	for {
		if kv.killed() {
			return
		}
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				index := msg.CommandIndex
				op := msg.Command.(Op)
				if !kv.ifDuplicate(op.ClientId, op.SeqId) {
					kv.mu.Lock()
					switch op.OpType {
					case "Put":
						kv.kvPersist[op.Key] = op.Value
						//DPrintf("S%d put后，结果为%v",kv.me, kv.kvPersist[op.Key])
					case "Append":
						kv.kvPersist[op.Key] += op.Value
						//DPrintf("Append后，结果为%v", kv.kvPersist[op.Key])
					}
					kv.seqMap[op.ClientId] = op.SeqId
					// 如果需要日志的容量达到规定值则需要制作快照并且投递
					if kv.isNeedSnapshot() {
						go kv.makeSnapshot(msg.CommandIndex) //索引是多少？lii是被压缩的最大索引
					}
					kv.mu.Unlock()
				}
				//返回请求结果
				kv.getWaitCh(index) <- op
			} else if msg.SnapshotValid {
				kv.decodeSnapshot(msg.SnapshotIndex, msg.Snapshot)
			}
		}
	}
}

// getWaitCh根据索引获取对应请求的等待channel，不存在则创建
func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan Op, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

// ifDuplicate根据clientId:seqId查询请求是否重复，重复返回true，不重复返回false
// 原理在于seqMap作为map[int64]int类型存储每个客户端已处理的请求的最高序号，只需要检查seqId<=kv.seqMap[clientId]
func (kv *KVServer) ifDuplicate(clinetId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId, exist := kv.seqMap[clinetId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
