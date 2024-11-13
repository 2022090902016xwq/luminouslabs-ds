package kvraft

import (
	"bytes"
	"luminouslabs-ds/labgob"
)

// 判断是否需要生成快照，如果需要则返回true，否则返回false
// 首先检查maxradtstate是否为-1，如果是，则不需要生成快照，直接返回false
// 否则获取raftstatesize比较是否要压缩生成快照
func (kv *KVServer) isNeedSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	Rlen := kv.persister.RaftStateSize()
	return Rlen-100 >= kv.maxraftstate
}

// 从kvPersist编码生成快照
func (kv *KVServer) makeSnapshot(index int) {
	//whether leader
	_, isleader := kv.rf.GetState()
	if !isleader {
		//DPrintf("kv.makeSnapshot not a leader\n")
		return
	}

	//writer and encoder
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	e.Encode(kv.kvPersist)
	e.Encode(kv.seqMap)
	snapshot := w.Bytes()
	DPrintf("kv.makeSnapshot index:%v encode a snapshot:%v \n", index, snapshot)
	go kv.rf.Snapshot(index, snapshot)
}

// 解码快照应用到kvPersist
func (kv *KVServer) decodeSnapshot(index int, snapshot []byte) {
	//判空，节点第一次启动会触发解码快照，不能继续读取空数据
	if snapshot == nil || len(snapshot) < 1 {
		DPrintf("kv.decodeSnapshot snapshot is nil\n")
		return
	}
	DPrintf("kv.decodeSnapshot snapshot:%v \n", snapshot)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.lastIncludeIndex = index

	if d.Decode(&kv.kvPersist) != nil || d.Decode(&kv.seqMap) != nil {
		DPrintf("kv.decodeSnapshot readPersist snapshot decode error\n")
		panic("error in parsing snapshot")
	}
}
