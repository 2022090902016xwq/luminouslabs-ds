package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// TaskArgs rpc请求传入的参数
type TaskArgs struct{}

// Task rpc的回应，worker根据接收的任务进行操作
type Task struct {
	TaskType   TaskType //任务类型Map或Reduce
	TaskID     int      //任务的ID
	ReducerNum int      //reducer的数量
	Files      []string //传入的文件
}
type TaskType int

// 任务类型
const (
	TaskMap     TaskType = iota //Map
	TaskReduce                  //Reduce
	TaskWaiting                 //Map或Reduce任务被分发完了，任务是等待
	TaskExit                    //任务完成，worker退出
)

// 阶段类型
const (
	PhaseMap    Phase = iota //Map
	PhaseReduce              //Reduce
	AllDone                  //全部完成
)

type Phase int

// 任务状态
const (
	Working State = iota //任务正在进行
	Waiting              //任务等待执行
	Done                 //任务完成
)

type State int

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
