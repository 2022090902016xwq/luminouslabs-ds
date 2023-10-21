package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	keep := true
	for keep {
		Task := CallGetTask()
		switch Task.TaskType {
		case TaskMap:
			DoTaskMap(mapf, &Task)
			CallDone(&Task)
		case TaskReduce:
			DoTaskReduce(reducef, &Task)
			CallDone(&Task)
		case TaskWaiting:
			time.Sleep(time.Second)
		case TaskExit:
			keep = false
		}

	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func DoTaskMap(mapf func(string, string) []KeyValue, reply *Task) {
	/*输入文件*/
	//从reply中读取文件名字，一个Map对应一个文件
	filename := reply.Files[0]
	//打开文件
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	//获取文件内容
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	/*执行Map操作*/
	//获得结构体数组（中间键值对的数组）
	intermediate := mapf(filename, string(content))

	/*hash处理*/
	//将intermediate划分为rn个部分,存到二维数组hashKV中
	rn := reply.ReducerNum
	hashKV := make([][]KeyValue, rn)
	for _, kv := range intermediate {
		hashKV[ihash(kv.Key)%rn] = append(hashKV[ihash(kv.Key)%rn], kv)
	}

	/*JSON序列化后存储*/
	for i := 0; i < rn; i++ {
		//命名文件
		oname := "mr-temp-" + strconv.Itoa(reply.TaskID) + "-" + strconv.Itoa(i)
		//创建文件
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create %v", oname)
		}
		//内容存储
		for _, kv := range hashKV[i] {
			//JSON序列化，kv存到ofile中
			err := json.NewEncoder(ofile).Encode(kv)
			if err != nil {
				return
			}
		}
		ofile.Close()
	}

}
func DoTaskReduce(reducef func(string, []string) string, reply *Task) {

	reduceFileNum := reply.TaskID
	ofilename := "mr-out-" + strconv.Itoa(reduceFileNum)
	ofile, _ := os.Create(ofilename)
	intermediate := shuffle(reply.Files)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}
func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

func CallDone(task *Task) {
	args := task
	reply := Task{}
	ok := call("Coordinator.MarkFinished", args, &reply)
	if ok {
		// fmt.Printf("Task mark finished!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallGetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		// fmt.Printf("get task!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
