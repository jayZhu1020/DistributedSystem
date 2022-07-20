/*
* Notes on the implementation:
* The map and reduce tasks output is written to a global file system rather than a local file system.
* this implementation works on a single machine but will fail if run on clusters.
* Extra communication mechanism needs to be implemented to achieve this.
 */
package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

var (
	numReduceTasks int
)

const waitDurationSecond = 15

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
		return AskForMapReduceTaskReply{TaskType: LoseConnTask}
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
	switch reply.TaskType {
	case MapTask:
		log.Printf("Receive Map Task on filename %v\n", reply.TaskDetail[0])
		doMapTask(mapf, reply)
		return false
	case ReduceTask:
		log.Printf("Receive Map Task on filename %v to %v\n", reply.TaskDetail[0], reply.TaskDetail[len(reply.TaskDetail)-1])
		doReduceTask(reducef, reply)
		return false
	case WaitTask:
		log.Println("Receive Wait Task")
		doWaitTask()
		return false
	case ExitTask:
		log.Println("Receive Exit Task")
		return true
	case LoseConnTask:
		log.Println("Cannot connect to coordinator")
		return true
	default:
		log.Println("Receive Unknown Task")
		return true
	}
}

//
// When the corrdinator instructs to do map task, perform the maptask on the file
//
func doMapTask(mapf func(string, string) []KeyValue, reply AskForMapReduceTaskReply) {
	// open the input files
	fileName := reply.TaskDetail[0]
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Cannot open file %v\n", fileName)
	}
	// read the input file
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read %v\n", fileName)
	}
	// close the opened file
	if err := file.Close(); err != nil {
		log.Fatalf("Cannot close file %v\n", fileName)
	}

	// perform map reduce
	intermediate := mapf(fileName, string(content))

	// create nReduce temp map output file
	mapOutFiles := []*os.File{}
	for outReduceId := 0; outReduceId < numReduceTasks; outReduceId++ {
		currMapOutFileName := fmt.Sprintf(MapOutFileFormat, reply.TaskId, outReduceId)
		currMapOutFile, err := ioutil.TempFile("", currMapOutFileName)
		if err != nil {
			log.Fatalf("cannot create temp file %v\n", currMapOutFileName)
		}
		mapOutFiles = append(mapOutFiles, currMapOutFile)
	}

	// append to the output file based on the hash,
	// the output file has the format
	/*
		key1 val1
		key2 val2
		...
		keyn valn
	*/
	for _, kvPair := range intermediate {
		targetReduceId := ihash(kvPair.Key) % numReduceTasks
		// write output to file, where key and value are separated by space and different key value pairs are separated by newline
		_, err := fmt.Fprintf(mapOutFiles[targetReduceId], "%s %s\n", kvPair.Key, kvPair.Value)
		if err != nil {
			log.Fatalf("cannot write to temp file of %v\n", fmt.Sprintf(MapOutFileFormat, reply.TaskId, targetReduceId))
		}
	}

	// close and rename the files
	for outReduceId, file := range mapOutFiles {
		tempName := file.Name()
		targetName := fmt.Sprintf(MapOutFileFormat, reply.TaskId, outReduceId)
		err = file.Close()
		if err != nil {
			log.Fatalf("cannot close file %v\n", tempName)
		}
		err := os.Rename(tempName, targetName)
		if err != nil {
			log.Fatalf("cannot rename file %v\n", targetName)
		}
	}

	// task is done, instruct the coordinator
	reportTaskDone(reply)
}

//
// When the corrdinator instructs to do reduce task, perform the redice tasks on the files
//
func doReduceTask(reducef func(string, []string) string, reply AskForMapReduceTaskReply) {
	// read the intermediate map output file and store into a data strcutre
	reduceFileNames := reply.TaskDetail
	keyToList := make(map[string][]string)
	for _, name := range reduceFileNames {
		// open intermediate file
		mapIntermediateFile, err := os.Open(name)
		if err != nil {
			log.Fatalf("cannot open file %v\n", name)
		}
		scanner := bufio.NewScanner(mapIntermediateFile)
		// read the content line by line
		for scanner.Scan() {
			// extract key-val pairs
			var key, val string
			currText := scanner.Text()
			_, err := fmt.Sscanf(currText, "%s %s\n", &key, &val)
			if err != nil {
				log.Fatalf("cannot scan string %v", currText)
			}
			keyToList[key] = append(keyToList[key], val)
		}
		// close the file
		err = mapIntermediateFile.Close()
		if err != nil {
			log.Fatalf("cannot close file %v\n", name)
		}
	}
	// perform map on each key and output to a slice
	reduceResults := make(map[string]string)
	for key, values := range keyToList {
		currReduceResult := reducef(key, values)
		reduceResults[key] = currReduceResult
	}
	// create a temp output file
	reduceOutputFileName := fmt.Sprintf(ReduceOutFileFormat, reply.TaskId)
	reduceOutputFile, err := ioutil.TempFile("", reduceOutputFileName)
	reduceTempFileName := reduceOutputFile.Name()
	if err != nil {
		log.Fatalf("cannot create temp file %v\n", reduceTempFileName)
	}
	// write to that file
	for reduceKey, reduceResult := range reduceResults {
		fmt.Fprintf(reduceOutputFile, "%s %s\n", reduceKey, reduceResult)
	}
	// close the file
	err = reduceOutputFile.Close()
	if err != nil {
		log.Fatalf("cannot close temp file %v\n", reduceTempFileName)
	}
	// rename the file
	err = os.Rename(reduceTempFileName, reduceOutputFileName)
	if err != nil {
		log.Fatalf("cannot rename %s\n", reduceTempFileName)
	}

	// report to the coordinator
	reportTaskDone(reply)
}

//
// When the corrdinator instructs to wait, wait for fixed amount of seconds
//
func doWaitTask() {
	<-time.After(waitDurationSecond * time.Second)
}

//
// When the corrdinator instructs to wait, wait for fixed amount of milisecond
//
func reportTaskDone(prevReply AskForMapReduceTaskReply) {
	args := ReportTaskDoneArgs{}
	args.TaskType = prevReply.TaskType
	args.TaskId = prevReply.TaskId
	ok := call("Coordinator.ReportTaskDone", &args, &ReportTaskDoneReply{})
	if !ok {
		log.Fatalln("Unable to talk to coordinator")
	}
	switch prevReply.TaskType {
	case MapTask:
		log.Printf("report map task ")
	case ReduceTask:
		log.Printf("report reduce task ")
	}
	log.Printf("report task ")
}

//
// When the corrdinator instructs to wait, wait for fixed amount of milisecond
//

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
