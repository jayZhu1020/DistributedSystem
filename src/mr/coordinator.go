/*
* Notes on the implementation:
* 1. in the original paper, the master knows the information of the workers beforehand
* and ping them periodically for information. In this implementation, the coordinator process
* was invoked through RPC passively by workers. The coordinator has a thread that periodically checks
* the last timestamp a RPC call is happen on coordinator. If this takes too long, the coordinator aborts
*
 */
package mr

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Stack data structure
type stack []int

func (stk *stack) push(i int) {
	*stk = append(*stk, i)
}

func (stk *stack) pop() (int, bool) {
	if len(*stk) == 0 {
		return -1, false
	}
	lastidx := len(*stk) - 1
	ret := (*stk)[lastidx]
	*stk = (*stk)[:lastidx]
	return ret, true
}

// maximum second the coordinator can be idle
const maxIdleSecond = 15

const checkTaskDelaySecond = 10

const (
	idle = iota
	ongoing
	done
)

type Coordinator struct {
	idleMapTasksIds   list.List // encode in integers 0, ..., nFiles-1
	idleRduceTasksIds list.List // encocde in integers 0 ..., nReduce-1

	mapTaskIdToFiles    map[int][]string // associate map task id to the task filenames
	reduceTaskIdToFiles map[int][]string // associate reduce task id to the reduce filenames

	mapTasksStatus    map[int]int // associate map task id to status: idle, ongoing or done
	reduceTasksStatus map[int]int // associate map task id to status: idle, ongoing or done

	coordLock sync.Mutex // mutex on coordinator ensuring mutual exclusion

	numMapTasks    int // total number of map tasks
	numReduceTasks int // total number of reduce tasks

	lastActiveTime time.Time // last time that a process at the coordinator is called throught RPC
}

// Your code here -- RPC handlers for the worker to call.

//
// Return to the worker how many reduce tasks there are.
// This function is called whenever a worker process starts
//
func (c *Coordinator) GetNumReduceTasks(args *GetNumReduceTasksArgs, reply *GetNumReduceTasksReply) error {
	c.coordLock.Lock()
	defer c.coordLock.Unlock()
	c.lastActiveTime = time.Now()
	reply.NumReduceTasks = c.numReduceTasks
	log.Printf("Reply to worker with number of reduce tasks being %v\n", c.numReduceTasks)
	return nil
}

//
// Return to the worker what tasks to do
//
func (c *Coordinator) AskForMapReduceTask(args *AskForMapReduceTaskArgs, reply *AskForMapReduceTaskReply) error {
	c.coordLock.Lock()
	defer c.coordLock.Unlock()
	c.lastActiveTime = time.Now()
	log.Printf("askForMapReduceTask called at %v\n", c.lastActiveTime)

	// **TODO:** TO BE IMPLEMENTED

	return nil
}

//
// Report the work done with some filename
//
func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {

	// **TODO:** TO BE IMPLEMENTED

	return nil
}

//
// Report the work done with some filename
//
func (c *Coordinator) checkTaskDone(taskType int, taskId int) {
	c.coordLock.Lock()
	defer c.coordLock.Unlock()
	<-time.After(checkTaskDelaySecond * time.Second)

	// **TODO:** TO BE IMPLEMENTED

	if taskType == MapTask {
		if c.mapTasksStatus[taskId] == ongoing {

		}
	} else if taskType == ReduceTask {
		if c.reduceTasksStatus[taskId] == ongoing {

		}
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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
	log.Println("Starting a HTTP server")
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished. If the last active timestamp is at least 20 seconds, abort the coordinator
//
func (c *Coordinator) Done() bool {
	c.coordLock.Lock()
	defer c.coordLock.Unlock()
	ret := !c.lastActiveTime.IsZero() && time.Since(c.lastActiveTime).Seconds() > maxIdleSecond
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// initialize variables
	c := Coordinator{}
	c.numReduceTasks = nReduce
	c.numMapTasks = len(files)
	c.idleMapTasksIds = *list.New()
	c.idleRduceTasksIds = *list.New()
	c.mapTaskIdToFiles = make(map[int][]string)
	c.reduceTaskIdToFiles = make(map[int][]string)
	c.mapTasksStatus = make(map[int]int)
	c.reduceTasksStatus = make(map[int]int)
	for mapTaskIndex, mapTask := range files {
		c.idleMapTasksIds.PushBack(mapTaskIndex)
		c.mapTasksStatus[mapTaskIndex] = idle
		c.mapTaskIdToFiles[mapTaskIndex] = append(c.mapTaskIdToFiles[mapTaskIndex], mapTask)
	}
	for reduceId := 0; reduceId < nReduce; reduceId++ {
		c.idleRduceTasksIds.PushBack(reduceId)
		c.reduceTasksStatus[reduceId] = idle
		for mapId := 0; mapId < c.numMapTasks; mapId++ {
			c.reduceTaskIdToFiles[reduceId] = append(c.reduceTaskIdToFiles[reduceId], fmt.Sprintf(MapOutFileFormat, mapId, reduceId))
		}
	}
	// log the map tasks
	log.Printf("Initialize coordinator with %v map tasks and %v reduce tasks\n", c.numMapTasks, c.numReduceTasks)
	log.Printf("Map tasks:\n")
	for mapId, mapFiles := range c.mapTaskIdToFiles {
		log.Printf("Map task ID %v correspond to files:", mapId)
		for _, mapFile := range mapFiles {
			log.Printf("%v ", mapFile)
		}
		log.Println("")
	}
	log.Println("")
	// log the reduce tasks
	log.Printf("Reduce tasks:\n")
	for reduceId, reduceFiles := range c.reduceTaskIdToFiles {
		log.Printf("Reduce task ID %v correspond to files: ", reduceId)
		for _, reduceFile := range reduceFiles {
			log.Printf("%v ", reduceFile)
		}
		log.Println("")
	}
	c.server()
	return &c
}
