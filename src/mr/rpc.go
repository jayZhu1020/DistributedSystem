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

const (
	MapTask int = iota
	ReduceTask
	ExitTask
	WaitTask
	LoseConnTask
)

// the slot identifies from which reduce task this file outputs
const ReduceOutFileFormat = "mr-reduceout-%v"

// the first slot identifies from which map task this file outputs
// the second slot identifies to which reduce task to feed this file to
const MapOutFileFormat = "mr-mapout-%v-%v"

type GetNumReduceTasksArgs struct {
}

type GetNumReduceTasksReply struct {
	NumReduceTasks int
}

type AskForMapReduceTaskArgs struct {
}

type AskForMapReduceTaskReply struct {
	Task int // being MapTask, ReduceTask, ExitTask or WaitTask or LoseConnTask

	// task detail.
	// For a map task it will contain a single file name.
	// E.g. "pg-being_ernest.txt"
	// For a reduce task it will contain the it will contain numReduce filenames we want to perform reduce.
	// E.g. "mr-mapout-1-1.txt",...,"mr-mapout-1-{nReduce}.txt"
	TaskDetail []string
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
