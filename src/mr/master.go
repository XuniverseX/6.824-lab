package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus int //task的状态

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type Task struct {
	Input         string
	TaskState     State
	NReduce       int
	TaskNumber    int
	Intermediates []string
	Output        string
}

type TaskInfo struct {
	TaskStatus    TaskStatus
	StartTime     time.Time
	TaskReference *Task
}

type Master struct {
	TaskQueue     chan *Task        // 等待执行的task
	TaskMeta      map[int]*TaskInfo // 当前所有task的信息
	MasterPhase   State             // Master的阶段
	NReduce       int
	InputFiles    []string
	Intermediates [][]string // Map任务产生的R个中间文件的信息
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	// Your code here.
	mu.Lock()
	ret := m.MasterPhase == Exit
	mu.Unlock()
	time.Sleep(2 * time.Second)
	return ret
}

func (m *Master) createMapTask() {
	//每个文件对应一个mapTask
	for i, filename := range m.InputFiles {
		task := Task{
			Input:      filename,
			TaskState:  Map,
			NReduce:    m.NReduce,
			TaskNumber: i,
		}
		m.TaskQueue <- &task
		m.TaskMeta[i] = &TaskInfo{
			TaskStatus:    Idle,
			TaskReference: &task,
		}
	}
}

func (m *Master) createReduceTask() {
	m.TaskMeta = make(map[int]*TaskInfo)
	for i, paths := range m.Intermediates {
		task := Task{
			TaskState:     Reduce,
			NReduce:       m.NReduce,
			TaskNumber:    i,
			Intermediates: paths,
		}
		m.TaskQueue <- &task
		m.TaskMeta[i] = &TaskInfo{
			TaskStatus:    Idle,
			TaskReference: &task,
		}
	}
}

func (m *Master) catchTimeOut() {
	for {
		time.Sleep(2 * time.Second)
		mu.Lock()
		if m.MasterPhase == Exit {
			mu.Unlock()
			return
		}
		for _, masterTask := range m.TaskMeta {
			if masterTask.TaskStatus == InProgress && time.Now().Sub(masterTask.StartTime) >= 10*time.Second {
				// 因为worker写在同一个文件磁盘上，对于重复的结果要丢弃
				m.TaskQueue <- masterTask.TaskReference
				masterTask.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		TaskQueue:     make(chan *Task, max(nReduce, len(files))),
		TaskMeta:      make(map[int]*TaskInfo),
		MasterPhase:   Map,
		NReduce:       nReduce,
		InputFiles:    files,
		Intermediates: make([][]string, nReduce),
	}

	// 切成16MB-64MB的文件
	// 创建map任务
	m.createMapTask()

	// 一个程序成为master，其他成为worker
	//这里就是启动master 服务器就行了，
	//拥有master代码的就是master，别的发RPC过来的都是worker
	m.server()
	// 启动一个goroutine 检查超时的任务
	go m.catchTimeOut()
	return &m
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// AssignTask master等待worker调用
func (m *Master) AssignTask(req *AssignTaskReq, resp *AssignTaskResp) error {
	mu.Lock()
	defer mu.Unlock()
	if len(m.TaskQueue) > 0 {
		task := <-m.TaskQueue
		resp.Task = task
		m.TaskMeta[task.TaskNumber].TaskStatus = InProgress
		m.TaskMeta[task.TaskNumber].StartTime = time.Now()
	} else if m.MasterPhase == Exit {
		resp.Task = &Task{TaskState: Exit}
	} else {
		//没有task，让worker等待
		resp.Task = &Task{TaskState: Wait}
	}
	return nil
}

// TaskCompleted 任务结束worker远程调用
func (m *Master) TaskCompleted(req *TaskCompletedReq, resp *TaskCompletedResp) error {
	mu.Lock()
	defer mu.Unlock()

	task := req.Task
	if task.TaskState != m.MasterPhase || m.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		// 因为worker写在同一个文件磁盘上，对于重复的结果要丢弃
		return nil
	}
	//更改task状态
	m.TaskMeta[task.TaskNumber].TaskStatus = Completed
	go m.processTaskResult(task)
	return nil
}

// 处理任务结果
func (m *Master) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		for reduceId, filepath := range task.Intermediates {
			m.Intermediates[reduceId] = append(m.Intermediates[reduceId], filepath)
		}
		if m.allTaskDone() {
			//进入reduce阶段
			m.createReduceTask()
			m.MasterPhase = Reduce
		}
	case Reduce:
		if m.allTaskDone() {
			//master退出
			m.MasterPhase = Exit
		}
	}
}

// 该阶段所有任务是否执行完
func (m *Master) allTaskDone() bool {
	for _, info := range m.TaskMeta {
		if info.TaskStatus != Completed {
			return false
		}
	}
	return true
}
