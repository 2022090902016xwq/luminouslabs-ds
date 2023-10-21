package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int
	DistPhase         Phase
	TaskMapChannel    chan *Task
	TaskReduceChannel chan *Task
	TaskID            int
	TaskSequence      TaksHolder
}
type TaksHolder struct {
	Meta map[int]*TaskInfo
}
type TaskInfo struct {
	state     State
	StratTime time.Time
	TaskAdr   *Task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.DistPhase {
	case PhaseMap:
		{
			if len(c.TaskMapChannel) > 0 {
				*reply = *<-c.TaskMapChannel
				c.TaskSequence.changeTaskState(reply.TaskID)
			} else {
				reply.TaskType = TaskWaiting
				if c.TaskSequence.checkTaskDone() {
					c.toNextPhase()
				}
			}
		}
	case PhaseReduce:
		{
			if len(c.TaskReduceChannel) > 0 {
				*reply = *<-c.TaskReduceChannel
				c.TaskSequence.changeTaskState(reply.TaskID)
			} else {
				reply.TaskType = TaskWaiting
				if c.TaskSequence.checkTaskDone() {
					c.toNextPhase()
				}
			}
		}
	case AllDone:
		reply.TaskType = TaskExit
	default:
		panic("No such phase!")
	}
	return nil
}
func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	id := args.TaskID
	meta := c.TaskSequence.Meta[id]
	if meta.state == Working {
		meta.state = Done
		return nil
	} else {
		err := errors.New("error :Not finished")
		return err
	}
}

// 检查所有任务的完成情况
func (h *TaksHolder) checkTaskDone() bool {
	var (
		mapDone      int = 0
		mapUnDone    int = 0
		reduceDone   int = 0
		reduceUnDone int = 0
	)
	for _, v := range h.Meta {
		if v.TaskAdr.TaskType == TaskMap {
			if v.state == Done {
				mapDone++
			} else {
				mapUnDone++
			}
		} else if v.TaskAdr.TaskType == TaskReduce {
			if v.state == Done {
				reduceDone++
			} else {
				reduceUnDone++
			}
		}
	}
	//map阶段任务完成
	if (mapDone > 0 && mapUnDone == 0) && (reduceDone == 0 && reduceUnDone == 0) {
		return true
	} else if reduceDone > 0 && reduceUnDone == 0 { //reduce阶段任务完成
		return true
	}
	return false
}

// 修改任务状态
func (h *TaksHolder) changeTaskState(taskID int) {

	taskInfo := h.Meta[taskID]
	if taskInfo.state == Waiting {
		taskInfo.state = Working
		taskInfo.StratTime = time.Now()
	}
}
func (c *Coordinator) toNextPhase() {
	if c.DistPhase == PhaseMap {
		c.makeReduceTasks()
		c.DistPhase = PhaseReduce
	} else if c.DistPhase == PhaseReduce {
		c.DistPhase = AllDone
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool { //竞争处理不到位不理解
	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		return true
	} else {
		return false
	}

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		ReducerNum:        nReduce,
		DistPhase:         PhaseMap,
		TaskMapChannel:    make(chan *Task, len(files)),
		TaskReduceChannel: make(chan *Task, nReduce),
		TaskSequence: TaksHolder{
			make(map[int]*TaskInfo, len(files)+nReduce),
		},
	}

	// Your code here.
	c.makeMapTasks(files)
	// fmt.Println("MapTask allready")
	c.server()
	// fmt.Println("Waiting Worker")
	go c.crashDetecter()

	return &c
}
func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		// fmt.Println("The file is ", v)
		id := c.generateTaskID()
		task := Task{
			TaskType:   TaskMap,
			TaskID:     id,
			ReducerNum: c.ReducerNum,
			Files:      []string{v},
		}
		taskInfo := TaskInfo{
			state:   Waiting,
			TaskAdr: &task,
		}
		c.TaskSequence.storageTask(&taskInfo)
		// fmt.Println(c.TaskSequence)
		c.TaskMapChannel <- &task
	}
	// fmt.Println("Map Channel ", c.TaskMapChannel)
}
func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskID()
		task := Task{
			TaskType:   TaskReduce,
			TaskID:     id,
			ReducerNum: c.ReducerNum,
			Files:      selectReduceName(i),
		}
		taskInfo := TaskInfo{
			state:   Waiting,
			TaskAdr: &task,
		}
		c.TaskSequence.storageTask(&taskInfo)
		c.TaskReduceChannel <- &task
	}
}
func selectReduceName(i int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := os.ReadDir(path)
	for _, fi := range files {
		if strings.HasPrefix(fi.Name(), "mr-temp-") && strings.HasSuffix(fi.Name(), strconv.Itoa(i)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

func (c *Coordinator) generateTaskID() int {
	id := c.TaskID
	c.TaskID++
	return id
}
func (h *TaksHolder) storageTask(taskInfo *TaskInfo) bool {
	taskId := taskInfo.TaskAdr.TaskID
	meta := h.Meta[taskId]
	if meta != nil {
		//fmt.Println("meta contains task which id = ", taskId)
		return false
	} else {
		h.Meta[taskId] = taskInfo
	}
	return true
}
func (c *Coordinator) crashDetecter() {
	for {
		time.Sleep(2 * time.Second)
		// fmt.Println("carsh detecting")
		mu.Lock()
		if c.DistPhase == AllDone {
			mu.Unlock()
			break
		}
		for _, v := range c.TaskSequence.Meta {
			// fmt.Println("---Get TaskSequence ", v)
			if v.state == Working && time.Since(v.StratTime) > 9*time.Second {
				switch v.TaskAdr.TaskType {
				case TaskMap:
					{
						c.TaskMapChannel <- v.TaskAdr
						v.state = Waiting
					}
				case TaskReduce:
					{
						{
							c.TaskReduceChannel <- v.TaskAdr
							v.state = Waiting
						}
					}
				}
			}
		}
		mu.Unlock()
	}
}
