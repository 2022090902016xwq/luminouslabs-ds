package kvraft

import (
	"luminouslabs-ds/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seqId    int   //请求的序列号，初始化从0开始，每一次请求加1
	leaderID int   //记录上次的leader，优先将所有请求发往leader进行处理
	clientId int64 //客户端的唯一标识
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderID = int(nrand()) % len(servers)
	//ck.seqId = 0
	DPrintf("MakeClerk clientId:%d", ck.clientId)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	// init arg and reply,then call RPC and handle reply
	ck.seqId++
	serverId := ck.leaderID
	args := &GetArgs{Key: key, ClientId: ck.clientId, SeqId: ck.seqId}

	for {
		reply := &GetReply{} //make sure empty
		ok := ck.servers[serverId].Call("KVServer.Get", args, reply)

		if ok { //call调用服务端正常
			if reply.Err == OK { //正确处理
				ck.leaderID = serverId
				return reply.Value
			} else if reply.Err == ErrNoKey {
				ck.leaderID = serverId
				return ""
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}

		}

		//节点crash等情况
		serverId = (serverId + 1) % len(ck.servers)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId++
	serverId := ck.leaderID
	args := &PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, SeqId: ck.seqId}

	for {
		reply := &PutAppendReply{} //make sure empty
		ok := ck.servers[serverId].Call("KVServer.PutAppend", args, reply)

		if ok { //call调用服务端正常
			if reply.Err == OK { //正确处理
				ck.leaderID = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}

		//节点crash等情况
		serverId = (serverId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
