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
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// maximum second the coordinator can be idle
const maxIdleSecond = 20

const checkTaskDelaySecond = 10

const (
	idle = iota
	ongoing
	done
)

var intToStringStatus = map[int]string{
	idle:    "idle",
	ongoing: "ongoing",
	done:    "done",
}

type Coordinator struct {
	mapTasksIds    []int // encode in integers 0, ..., nFiles-1
	reduceTasksIds []int // encocde in integers 0 ..., nReduce-1

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
	log.Printf("worker asks for reduce task number at %v\n", c.lastActiveTime)
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
	log.Printf("worker asks for task at time %v\n", c.lastActiveTime)

	// **TODO:** TO BE IMPLEMENTED

	// if still idle map task left, assign a map task
	if countStatus(c.mapTasksStatus, idle) > 0 {
		assignedTaskId := findFirstWithStatus(c.mapTasksStatus, idle)
		reply.TaskType = MapTask
		reply.TaskId = assignedTaskId
		reply.TaskDetail = c.mapTaskIdToFiles[assignedTaskId]
		c.mapTasksStatus[assignedTaskId] = ongoing
		go c.checkTaskDone(reply.TaskType, reply.TaskId)

		log.Printf("assigned worker a map task with ID %v\n", reply.TaskId)
		log.Println("current status of map tasks:")
		logStatus(c.mapTasksStatus)
		return nil
	}
	// if all map tasks are assigned with some ongoing tasks
	// instruct the worker to wait
	if countStatus(c.mapTasksStatus, ongoing) > 0 {
		log.Printf("all map tasks are ongoing, instruct the worker to wait\n")
		reply.TaskType = WaitTask
		return nil
	}
	// otherwise all map tasks must have been done.
	// check if any reduce task left
	if countStatus(c.reduceTasksStatus, idle) > 0 {
		assignedTaskId := findFirstWithStatus(c.reduceTasksStatus, idle)
		reply.TaskType = ReduceTask
		reply.TaskId = assignedTaskId
		reply.TaskDetail = c.reduceTaskIdToFiles[assignedTaskId]
		c.reduceTasksStatus[assignedTaskId] = ongoing
		go c.checkTaskDone(reply.TaskType, reply.TaskId)
		log.Printf("assigned worker a reduce task with ID %v\n", reply.TaskId)
		log.Println("current status of reduce tasks:")
		logStatus(c.reduceTasksStatus)
		return nil
	}
	// otherwise all reduces tasks are done.
	// ask the worker to wait until we are sure all tasks are done
	if countStatus(c.reduceTasksStatus, ongoing) > 0 {
		log.Printf("all reduce tasks are ongoing, instruct the worker to wait\n")
		reply.TaskType = WaitTask
		return nil
	}
	// otherwise all map and reduce tasks are done. Assign an exit task
	log.Printf("all mapreduce tasks are done, instruct the worker to exit\n")
	reply.TaskType = ExitTask
	return nil
}

//
// Report the work done with some filename
//
func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	c.coordLock.Lock()
	defer c.coordLock.Unlock()
	c.lastActiveTime = time.Now()
	// **TODO:** TO BE IMPLEMENTED
	log.Printf("worker reports task finished at time %v\n", c.lastActiveTime)
	if args.TaskType == MapTask {
		log.Printf("map task %v finished\n", args.TaskType)
		c.mapTasksStatus[args.TaskId] = done
		return nil
	}
	if args.TaskType == ReduceTask {
		log.Printf("reduce task %v finished\n", args.TaskType)
		c.reduceTasksStatus[args.TaskId] = done
		return nil
	}

	log.Fatalln("Unknown task reported")
	return nil
}

//
// Check if the task is done after some second
// if it is still ongoing for a long period of time,
// it might be that the worker is down or hardly making progress
// then reset the task status to idle.
//
func (c *Coordinator) checkTaskDone(taskType int, taskId int) {
	<-time.After(checkTaskDelaySecond * time.Second)

	c.coordLock.Lock()
	defer c.coordLock.Unlock()
	// **TODO:** TO BE IMPLEMENTED
	if taskType == MapTask {
		log.Printf("Check the task status of map task %v\n", taskId)
		if c.mapTasksStatus[taskId] == ongoing {
			log.Printf("map task %v timeout, reset to idle\n", taskId)
			c.mapTasksStatus[taskId] = idle
			return
		}
		log.Printf("map task %v successfully performed within timeout\n", taskId)
	} else {
		log.Printf("Check the task status of reduce task %v\n", taskId)
		if c.reduceTasksStatus[taskId] == ongoing {
			log.Printf("reduce task %v timeout, reset to idle\n", taskId)
			c.reduceTasksStatus[taskId] = idle
			return
		}
		log.Printf("reduce task %v successfully performed within timeout\n", taskId)
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
	isOverMaxIdleTime := !c.lastActiveTime.IsZero() && time.Since(c.lastActiveTime).Seconds() > maxIdleSecond
	finishedAllTask := countStatus(c.mapTasksStatus, done) == c.numMapTasks && countStatus(c.reduceTasksStatus, done) == c.numReduceTasks
	ret := isOverMaxIdleTime || finishedAllTask
	if ret {
		log.Printf("time elapse since last active time in second: %v, ", time.Since(c.lastActiveTime).Seconds())
		log.Printf("Coordinator exiting")
	}
	return ret
}

//
// helper function that counts how many elements in the list
// is in a particular status
//
func countStatus(list map[int]int, status int) int {
	count := 0
	for _, v := range list {
		if v == status {
			count++
		}
	}
	return count
}

//
// helper function that counts how many elements in the list
// is in a particular status
//
func findFirstWithStatus(list map[int]int, status int) int {
	for k, v := range list {
		if v == status {
			return k
		}
	}
	return -1
}

//
// helper functon that logs the task status
//
func logStatus(m map[int]int) {
	for k, v := range m {
		log.Printf("taskId: %v, status: %v, ", k, intToStringStatus[v])
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// initialize containers and integer counts
	c := Coordinator{}
	c.numReduceTasks = nReduce
	c.numMapTasks = len(files)
	c.mapTaskIdToFiles = make(map[int][]string)
	c.reduceTaskIdToFiles = make(map[int][]string)
	c.mapTasksStatus = make(map[int]int)
	c.reduceTasksStatus = make(map[int]int)

	// initialize map tasks
	for mapTaskIndex, mapTask := range files {
		c.mapTasksIds = append(c.mapTasksIds, mapTaskIndex)
		c.mapTasksStatus[mapTaskIndex] = idle
		c.mapTaskIdToFiles[mapTaskIndex] = append(c.mapTaskIdToFiles[mapTaskIndex], mapTask)
	}

	// initialize reduce tasks
	for reduceTaskId := 0; reduceTaskId < nReduce; reduceTaskId++ {
		c.reduceTasksIds = append(c.reduceTasksIds, reduceTaskId)
		c.reduceTasksStatus[reduceTaskId] = idle
		for mapId := 0; mapId < c.numMapTasks; mapId++ {
			c.reduceTaskIdToFiles[reduceTaskId] = append(c.reduceTaskIdToFiles[reduceTaskId], fmt.Sprintf(MapOutFileFormat, mapId, reduceTaskId))
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
	}

	// log the reduce tasks
	log.Printf("Reduce tasks:\n")
	for reduceId, reduceFiles := range c.reduceTaskIdToFiles {
		log.Printf("Reduce task ID %v correspond to files: ", reduceId)
		for _, reduceFile := range reduceFiles {
			log.Printf("%v ", reduceFile)
		}
	}
	c.server()
	return &c
}
