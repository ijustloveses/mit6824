package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sync"
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

func send_request(cid string, cur_job string, cur_status string, reply *HeartbeatReply) bool {
	args := HeartbeatArgs{}
	args.Cid = cid
	args.Cur_job = cur_job
	args.Cur_status = cur_status
	return call("Master.Heartbeat", &args, reply)
}

func do_work(mu *sync.Mutex, cid string, job string, cur_status *string) {
	fmt.Println(cid + " is running " + job + " ...")
	time.Sleep(time.Second * 5)
	fmt.Println(cid + " finish " + job)
	mu.Lock()
	*cur_status = "done"
	mu.Unlock()
}

func Worker() {
	cid := "0"
	cur_job := ""
	cur_status := "idle"
	reply := HeartbeatReply{}
	mu := sync.Mutex{}

	for {
		mu.Lock()
		is_ok := send_request(cid, cur_job, cur_status, &reply)
		if is_ok {
			if cid == "0" {
				if reply.Cid == "0" {
					log.Fatal("Master didn't assign a valid cid")
					mu.Unlock()
					break
				}
				cid = reply.Cid
				if reply.Job_assigned != "" {
					cur_job = reply.Job_assigned
					cur_status = "ongoing"
					go do_work(&mu, cid, cur_job, &cur_status)
				}
			} else {
				if cid != reply.Cid {
					log.Fatal("cid mismatch: " + cid + " v.s. " + reply.Cid)
					mu.Unlock()
					break
				}
				if cur_status == "done" {
					if cur_job == "" {
						log.Fatal("Current job is null when worker status is done")
						mu.Unlock()
						break
					}
					if reply.Job_assigned != "" {
						cur_job = reply.Job_assigned
						cur_status = "ongoing"
						go do_work(&mu, cid, cur_job, &cur_status)
					} else {
						cur_job = ""
						cur_status = "idle"
					}
				} else if cur_status == "idle" {
					if cur_job != "" {
						log.Fatal("Current job is not null when worker status is idle")
						mu.Unlock()
						break
					}
					if reply.Job_assigned != "" {
						cur_job = reply.Job_assigned
						cur_status = "ongoing"
						go do_work(&mu, cid, cur_job, &cur_status)
					}
				} else { // ongoing
					if cur_job != reply.Job_assigned {
						log.Fatal("Current job not match the job sent to master")
						mu.Unlock()
						break
					}
				}
			}
		} else {
			log.Fatal("Something is wrong")
			mu.Unlock()
			time.Sleep(time.Second * 3)
			break
		}
		mu.Unlock()
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
