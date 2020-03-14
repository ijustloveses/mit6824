package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
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

//
// KeyValues -> file
//
func saveKVToFile(kvs []KeyValue, file string) error {
	fp, err := os.Open(file)
	defer fp.Close()
	if err != nil {
		panic(err)
	}
	enc := json.NewEncoder(fp)
	for _, kv := range kvs {
		err := enc.Encode(&kv)
		if err != nil {
			return err
		}
	}
	return nil
}

//
// file -> KeyValues
//
func loadKVFromFile(file string) ([]KeyValue, error) {
	fp, err := os.Open(file)
	defer fp.Close()
	if err != nil {
		panic(err)
	}
	kvs := []KeyValue{}
	dec := json.NewDecoder(fp)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			return kvs, err
		}
		kvs = append(kvs, kv)
	}
}

//
// main/mrworker.go calls this function.
/*
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

}
*/

func Worker() {
	work_id := 0

	for i := 0; i < 40; i++ {
		is_ok := call("Master.Heartbeat", work_id, &work_id)
		if is_ok {
			fmt.Println("id: ", work_id)
		} else {
			fmt.Println("Something is wrong")
			break
		}
		time.Sleep(time.Second)
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := masterSock()
	// c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
