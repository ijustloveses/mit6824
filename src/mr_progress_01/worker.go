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

/* worker 逻辑：
   1. 每个 worker 的主线程运行 Worker() 函数，其中维护 cid / cur_job / cur_status 3 个变量和一把锁
   2. worker 连接 master 会使用以上三个变量来构造 HeartbeatArgs 实例，汇报它当前的状态和任务
   3. worker 这 3 个变量之间的约束关系已经由 rpc/validate_heartbeat_args 检验，本文件不再检查
   4. 主线程中会通过 Heartbeat 请求的返回值来设置 cid 和 cur_job，并根据当前状态设置 cur_status
   5. 主线程从 master 获取新任务后，会启动新的 goroutine 来运行任务，任务结束后，协程会设置 cur_status
   6. 由 4 & 5，我们需要给主线程的每次处理请求的流程加锁，同时对处理任务协程调整 cur_status 的地方加锁

   缺陷：
   当无法 dial master 或者 master 对请求的返回有错时，说明网络错误或者程序逻辑错误，那么直接结束 worker
   以后，在无法 dial master 的情况下，需要加入重连的机制，而不是直接结束
*/

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
	cid := "0"
	cur_job := nojob
	cur_status := "idle"
	mu := sync.Mutex{}

	for {
		reply := HeartbeatReply{}
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
				if reply.Job_assigned != nojob { // 初次连接后被分配任务
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
					if reply.Job_assigned != nojob { // 任务完成后被分配新任务
						cur_job = reply.Job_assigned
						cur_status = "ongoing"
						go do_work(&mu, cid, cur_job, &cur_status)
					} else { // 任务完成后，没有新任务被分配
						cur_job = nojob
						cur_status = "idle"
					}
				} else if cur_status == "idle" {
					if reply.Job_assigned != nojob { // 处于 idle 状态的 worker 被分配任务
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
			break
		}
		mu.Unlock()
		time.Sleep(time.Second)
	}
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

func send_request(cid string, cur_job string, cur_status string, reply *HeartbeatReply) bool {
	args := HeartbeatArgs{}
	args.Cid = cid
	args.Cur_job = cur_job
	args.Cur_status = cur_status
	ret := call("Master.Heartbeat", &args, reply)
	fmt.Println("InOut: " + cid + " (" + cur_job + " " + cur_status + ")  -->  " + reply.Cid + " (" + reply.Job_assigned + ")")
	return ret
}

func do_work(mu *sync.Mutex, cid string, job string, cur_status *string) {
	fmt.Println(cid + " is running " + job + " ...")
	time.Sleep(time.Second * 5)
	fmt.Println(cid + " finish " + job)
	mu.Lock()
	*cur_status = "done"
	mu.Unlock()
}
