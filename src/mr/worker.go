package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task := GetTask()

		switch task.TaskState {
		case Map:
			mapper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(2 * time.Second)
		case Exit:
			return
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func mapper(task *Task, mapf func(string, string) []KeyValue) {
	content, err := ioutil.ReadFile(task.Input)
	if err != nil {
		log.Fatal("failed to read file: ", task.Input, err)
	}
	//一个文件的map中间结果
	intermediates := mapf(task.Input, string(content))

	//将map结果切分为NReduce份
	buffer := make([][]KeyValue, task.NReduce)
	for _, intermediate := range intermediates {
		index := ihash(intermediate.Key) % task.NReduce
		buffer[index] = append(buffer[index], intermediate)
	}

	var output []string
	for i := 0; i < task.NReduce; i++ {
		mapFilename := writeIntermediate(task.TaskNumber, i, buffer[i])
		output = append(output, mapFilename)
	}
	task.Intermediates = output
	TaskCompleted(task)
}

func reducer(task *Task, reducef func(string, []string) string) {
	intermediate := readFromIntermediates(task.Intermediates)
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", task.TaskNumber)
	file, err := os.OpenFile(oname, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		log.Fatal("failed to create file: ", oname, err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	file.Close()
	task.Output = oname
	TaskCompleted(task)

}

func readFromIntermediates(intermediates []string) []KeyValue {
	var kva []KeyValue
	for _, filePath := range intermediates {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatal("Failed to open file: ", filePath, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return kva
}

func writeIntermediate(x int, y int, kva []KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

	enc := json.NewEncoder(tempFile)
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempFile.Name(), outputName)
	return path.Join(dir, outputName)
}

func GetTask() Task {
	req := AssignTaskReq{}
	resp := AssignTaskResp{}
	call("Master.AssignTask", &req, &resp)

	return *resp.Task
}

func TaskCompleted(task *Task) {
	req := TaskCompletedReq{Task: task}
	resp := TaskCompletedResp{}
	call("Master.TaskCompleted", &req, &resp)
}

// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
