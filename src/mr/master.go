package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	taskList []tasks
	lock     sync.Mutex // to update tasks list
	nReduce  int        // #reduce
	nMap     int        // #map
}

type tasks struct {
	c         string    // m -> Map or r -> Reduce
	state     int       // 0 -> not started, 1 in progress, 2 done
	startTime time.Time // start time monitor 10 seconds
	input     string    // file name in map task
}

type TaskArgs struct {
	C          string // m -> Map or r -> Reduce
	Input      string // file name in map task
	TaskNumber int    // taskNumber in map or reduce 0-m, 0-r
	NReduce    int    // #reduce
	NMap       int    // #map
}

type TaskReport struct {
	C          string // m -> Map or r -> Reduce
	TaskNumber int    // m -> Map or r -> Reduce
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(reply *ExampleReply, args *TaskArgs) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	args.C = "none"
	for i := 0; i < len(m.taskList); i++ {
		if m.taskList[i].state == 0 {
			args.C = m.taskList[i].c
			args.Input = m.taskList[i].input
			args.TaskNumber = i
			if args.TaskNumber >= m.nMap {
				args.TaskNumber = i - m.nMap
			}
			args.NReduce = m.nReduce
			args.NMap = m.nMap

			m.taskList[i].state = 1
			m.taskList[i].startTime = time.Now()
			// fmt.Printf("%v master GetTask = %+v\n", time.Now(), args)
			return nil
		}
	}
	// fmt.Printf("%v master GetTask = %+v\n", time.Now(), args)
	return nil // need to think about this
}

func (m *Master) ReportTask(report *TaskReport, reply *ExampleReply) error {
	// fmt.Printf("%v master ReportTask = %+v\n", time.Now(), report)
	idx := report.TaskNumber
	if report.C == "r" {
		idx = m.nMap + report.TaskNumber
	}
	m.lock.Lock()
	defer m.lock.Unlock()

	m.taskList[idx].state = 2 // made idempotent???
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	timeNow := time.Now()
	done := true
	// fmt.Println("master task check = ", m.taskList)
	for i := 0; i < len(m.taskList); i++ {
		if m.taskList[i].state != 2 {
			done = false
		}
		if m.taskList[i].state == 1 {
			if timeNow.Sub(m.taskList[i].startTime).Seconds() > 10 {
				m.taskList[i].state = 0
			}
		}
	}
	if done {
		fmt.Println("master done")
	}
	return done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.lock.Lock()

	mapTaskLen := len(files)
	taskLen := mapTaskLen + nReduce
	m.taskList = make([]tasks, taskLen)
	m.nReduce = nReduce
	m.nMap = mapTaskLen
	startTime := time.Now()
	for i := 0; i < len(m.taskList); i++ {
		if i < mapTaskLen {
			m.taskList[i].c = "m"
			m.taskList[i].input = files[i]
		} else {
			m.taskList[i].c = "r"
		}
		m.taskList[i].state = 0
		m.taskList[i].startTime = startTime
	}
	// fmt.Printf("master starting time = %v\n", time.Now())
	// fmt.Println(m.taskList, len(m.taskList), cap(m.taskList))
	m.lock.Unlock()
	m.server()
	return &m
}
