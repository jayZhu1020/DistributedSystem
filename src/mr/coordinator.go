/*
* Notes on the implementation:
* 1. in the original paper, the master knows the information of the workers beforehand
* and ping them periodically for information. In this implementation, the coordinator process
* was invoked through RPC passively by workers. The coordinator has a thread that periodically checks if
* no tasks are on ongoing for certain period of time. If so, the coordinator aborts.
*
 */
package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	idleMapTasks       []string
	idleRduceTasks     []string
	ongoingMapTasks    []string
	ongoingReduceTasks []string
	coordLock          sync.Mutex
	numReduceTasks     int
	numMapTasks        int
}

// Your code here -- RPC handlers for the worker to call.

// Return to the worker how many reduce tasks there are.
// This function is called whenever a worker process starts
func (c *Coordinator) GetNumReduceTasks(args *GetNumReduceTasksArgs, reply *GetNumReduceTasksReply) error {
	// Only reading from data, no lock necessary
	reply.NumReduceTasks = c.numReduceTasks
	log.Printf("Reply to worker with number of reduce tasks being %v\n", c.numReduceTasks)
	return nil
}

// Return to the worker what tasks to do
func (c *Coordinator) askForMapReduceTask(args *AskForMapReduceTaskArgs, reply *AskForMapReduceTaskReply) error {
	// need locking as it modifies the key variables
	c.coordLock.Lock()
	defer c.coordLock.Unlock()

	return nil
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
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.numReduceTasks = nReduce
	c.numMapTasks = len(files)
	for _, mapTask := range files {
		c.idleMapTasks = append(c.idleMapTasks, mapTask)
	}
	for reduceId := 1; reduceId <= nReduce; reduceId++ {
		c.idleMapTasks = append(c.idleRduceTasks, strconv.Itoa(reduceId))
	}
	log.Printf("Initialize coordinator with %v map tasks and %v reduce tasks", c.numMapTasks, c.numReduceTasks)
	c.server()
	return &c
}
