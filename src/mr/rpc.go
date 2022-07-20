/*
* Intermediate file structure:
* key1 val1
* key2 val2
* ...
* keyN valN
 */
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
// constants identifying the reply task type
//
const (
	MapTask int = iota
	ReduceTask
	ExitTask
	WaitTask
	LoseConnTask
)

//
// the map output structure
//
const MapOutFileFormat = "mr-intout-%v-%v" // fmt.Sprintf(MapOutFileFormat, mapId, reduceId)

//
// the reduce output structure
//
const ReduceOutFileFormat = "mr-out-%v"

//
// Args and reply structure for c.GetNumReduceTasks()
//
type GetNumReduceTasksArgs struct{}

type GetNumReduceTasksReply struct {
	NumReduceTasks int
}

//
// Args and reply structure for c.AskForMapReduceTaskArgs()
//
type AskForMapReduceTaskArgs struct{}

type AskForMapReduceTaskReply struct {
	TaskType int // being MapTask, ReduceTask, ExitTask or WaitTask or LoseConnTask
	TaskId   int // task id, used to report back to the coordinator
	// task detail.
	// For a map task it will contain a single file name.
	// E.g. "pg-being_ernest.txt"
	// For a reduce task it will contain the numReduce filenames we want to perform reduce.
	// E.g. "mr-mapout-1-1.txt",...,"mr-mapout-1-{nReduce}.txt"
	TaskDetail []string
}

//
// Args and reply structure for c.ReportTaskDone()
//
type ReportTaskDoneArgs struct {
	TaskType int
	TaskId   int
}

type ReportTaskDoneReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
