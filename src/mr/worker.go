package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func getIntermediateFileName(m int, r int) string {
	return "mr-int-" + strconv.Itoa(m) + "-" + strconv.Itoa(r)
}

func mapWorker(mapf func(string, string) []KeyValue, task TaskArgs) bool {
	file, err := os.Open(task.Input)
	if err != nil {
		log.Fatalf("cannot open %v", task.Input)
	}

	tempFile := make([]*os.File, task.NReduce)
	encoders := make([]*json.Encoder, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		cwd, _ := os.Getwd()
		tempFile[i], _ = ioutil.TempFile(cwd, getIntermediateFileName(task.TaskNumber, i))
		encoders[i] = json.NewEncoder(tempFile[i])
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Input)
	}

	file.Close()
	kva := mapf(task.Input, string(content))

	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NReduce
		if encoders[idx].Encode(&kv) != nil {
			log.Fatalf("cannot encode %v", kv)
		}
	}

	for i := 0; i < task.NReduce; i++ {
		fileName := tempFile[i].Name()
		tempFile[i].Close()
		os.Rename(fileName, getIntermediateFileName(task.TaskNumber, i))
	}
	return true
}

func reduceWorker(reducef func(string, []string) string, task TaskArgs) bool {
	intermediate := ByKey{}

	// get blocked if file doesnt exist atleast retry for 1 seconds and exit
	for i := 0; i < task.NMap; i++ {
		filename := getIntermediateFileName(i, task.TaskNumber)
		start := time.Now()
		file, err := os.Open(filename)
		for true {
			if err == nil {
				break
			}
			t := time.Now()
			if t.Sub(start).Seconds() < 1 {
				time.Sleep(time.Second / 10)
				file, err = os.Open(filename)
			} else {
				return false
			}
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}

			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	cwd, _ := os.Getwd()
	ofile, err := ioutil.TempFile(cwd, "mr-out-"+strconv.Itoa(task.TaskNumber))

	if err != nil {
		fmt.Println("file creation error", "mr-out-"+strconv.Itoa(task.TaskNumber))
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		if err != nil {
			fmt.Println("file write error", intermediate[i].Key, output)
		}
		i = j
	}

	tmpName := ofile.Name()
	ofile.Close()
	os.Rename(tmpName, "mr-out-"+strconv.Itoa(task.TaskNumber))
	return true
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := TaskArgs{}
		reply := ExampleReply{}
		if !call("Master.GetTask", &reply, &args) {
			fmt.Println("master exited, not able to contact")
			return
		}
		done := false
		if args.C == "m" {
			done = mapWorker(mapf, args)
		} else if args.C == "r" {
			done = reduceWorker(reducef, args)
		}
		if done {
			report := TaskReport{args.C, args.TaskNumber}
			if !call("Master.ReportTask", &report, &reply) {
				fmt.Println("master exited, not able to contact")
				return
			}
		} else {
			time.Sleep(time.Second / 2)
		}
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
