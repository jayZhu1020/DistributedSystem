/*
* Notes on the implementation:
* The map and reduce tasks output is written to a global file system rather than a local file system.
* this implementation works on a single machine but will fail if run on clusters.
* Extra communication mechanism needs to be implemented to achieve this.
 */
package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

var (
	numReduceTasks int
)

const waitDurationMs = 50

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// worker processes ask for the number of reduce task when they are spawned
	askForNumTasks()
	// repeat forever until we can exit
	for {
		// ask the coordinator for a task
		reply := askForMapReduceTask()
		// perform the indicated task
		canExit := performTask(reply, mapf, reducef)
		// if we can exit after reading the task, break out of the loop
		if canExit {
			break
		}
	}
	log.Println("Worker Exiting")

}

//
// Calls the RPC to ask the coordinator for the number of tasks
//
// This function will be called whenevr the worker is initialized
//
func askForNumTasks() {
	args := GetNumReduceTasksArgs{}
	reply := GetNumReduceTasksReply{}
	ok := call("Coordinator.GetNumReduceTasks", &args, &reply)
	if !ok {
		log.Fatal("Error calling GetNumReduceTasks()")
	}
	numReduceTasks = reply.NumReduceTasks
	log.Printf("Number of reduce tasks is %v\n", numReduceTasks)
}

//
// Calls the RPC to ask the coordinator for a task
//
// Return: A reply indicating the action that should be taken.
// 	reply.Task <- the type of the task
//	reply.TaskDetail <- details that are associated to this task
func askForMapReduceTask() AskForMapReduceTaskReply {
	args := AskForMapReduceTaskArgs{}
	reply := AskForMapReduceTaskReply{}
	ok := call("Coordinator.AskForMapReduceTask", &args, &reply)
	if !ok {
		return AskForMapReduceTaskReply{LoseConnTask, -1, []string{}}
	}
	log.Println("Receive a task from coordinator")
	return reply
}

//
// Take specific action based on the reply
//
// Return: whether the worker can exit after performing the task
//
func performTask(reply AskForMapReduceTaskReply, mapf func(string, string) []KeyValue, reducef func(string, []string) string) bool {
	if reply.Task == MapTask {
		log.Printf("Receive Map Task on filename %v", reply.TaskDetail[0])
		doMapTask(mapf, reply.TaskDetail)
		reportTaskDone(reply)
		return true // CHANGE TO FALSE WHEN TESTING IS DONE
		// return false
	} else if reply.Task == ReduceTask {
		log.Printf("Receive Map Task on filename %v to %v", reply.TaskDetail[0], reply.TaskDetail[len(reply.TaskDetail)-1])
		doReduceTask(reducef, reply.TaskDetail)
		reportTaskDone(reply)
		return false
	} else if reply.Task == WaitTask {
		log.Println("Receive Wait Task")
		doWaitTask()
		return false
	} else if reply.Task == ExitTask {
		log.Println("Receive Exit Task")
		return true
	} else if reply.Task == LoseConnTask {
		log.Println("Cannot connect to coordinator")
		return true
	} else {
		log.Println("Receive Unknown Task")
		return true
	}
}

//
// When the corrdinator instructs to do map task, perform the maptask on the file
//
func doMapTask(mapf func(string, string) []KeyValue, filenames []string) {

}

//
// When the corrdinator instructs to do reduce task, perform the redice tasks on the files
//
func doReduceTask(reducef func(string, []string) string, filenames []string) {

}

//
// When the corrdinator instructs to wait, wait for fixed amount of milisecond
//
func doWaitTask() {
	<-time.After(waitDurationMs * time.Millisecond)
}

//
// When the corrdinator instructs to wait, wait for fixed amount of milisecond
//
func reportTaskDone(prevReply AskForMapReduceTaskReply) {

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
