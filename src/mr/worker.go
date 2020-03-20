package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strconv"
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
   1. worker 的主线程运行 Worker() 函数，维护 cid / cur_job / cur_status / cur_phase / jobs 5 个变量和一把锁
	  其中，cur_status 是每次 heartbeat 之前由 cid + cur_job +jobs 三个变量的状态推断出来的
	       cur_phase 表示当前处于 map 还是 reduce 阶段
   2. worker 连接 master 会使用前三个变量来构造 HeartbeatArgs 实例，汇报它当前的状态和任务
      不需要传递 cur_phase 变量，是因为本实现下，这个变量其实是由 master 掌控并传递给 workers 的
   3. worker 前 3 个变量之间的约束关系已经由 rpc/validate_heartbeat_args 检验，本文件不再检查
   4. 主线程中会通过 Heartbeat 请求的返回值来设置 cid、cur_job 和 jobs，当然还有任务的文件参数
   5. 主线程从 master 获取新任务后，会启动新的 goroutine 来运行任务，任务结束后，协程会设置 jobs[job] 为 done
   6. 由 4 & 5，我们需要给主线程的每次处理请求的流程加锁，同时对处理任务协程调整 jobs 的地方加锁
   7. 当 worker 断线重连时，可能会从 master 获得重新分配的 cid 和任务，需要重新调整 cur_job 来覆盖当前任务
   8. 断线重连后获得新任务，此时如上一任务协程仍在运行，不会强制停止。由于 cur_job 已经调整，故此运行结果不会被发送给 master
   9. 当无法 dial master 或者 master 对请求的返回有错时，说明网络错误或者程序逻辑错误，1 秒后重新发送消息
   10. 总体来说，worker 部分的实现“几乎”是无状态的，严格执行 master 传入的任务
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

func check_worker_status(cid string, cur_job string, jobs map[string]string) (string, error) {
	if cur_job == nojob || cid == "0" {
		return "idle", nil
	}
	status, matched := jobs[cur_job]
	if matched {
		return status, nil
	} else {
		return "", errors.New("Failed to find cur_job in jobs")
	}
}

func Worker() {
	cid := "0"
	cur_job := nojob
	jobs := make(map[string]string) // 历史任务状态，状态包括 ongoing, done, ignoring
	mu := sync.Mutex{}

	for {
		mu.Lock()
		cur_status, err := check_worker_status(cid, cur_job, jobs)
		if err != nil {
			log.Fatal(err)
		} else {
			reply := HeartbeatReply{}
			is_ok := send_request(cid, cur_job, cur_status, &reply)
			// 如果请求失败，那么什么都不做，也不处理 reply，下一秒重连
			if is_ok {
				err = validate_heartbeat_reply(&reply) // 检查 reply 的合理性
				if err != nil {
					log.Fatal(err)
				} else {
					if cid == "0" {
						cid = reply.Cid
						cur_job = reply.Job_assigned
						if cur_job != nojob { // 初次连接后被分配任务
							jobs[cur_job] = "ongoing"
							go do_work(&mu, cid, cur_job, jobs, reply.Job_files, reply.Phase)
						}
					} else {
						if cid != reply.Cid { // 断线重连，获得新的 cid
							cid = reply.Cid
							// 如果分配的任务恰好和之前的任务一致，也就是说断线前的任务，那么什么也不做
							if reply.Job_assigned != cur_job {
								cur_job = reply.Job_assigned // 否则，cur_job 被覆盖为新任务，忘掉以前的任务
								if cur_job != nojob {        // 断线重连后被分配任务
									jobs[cur_job] = "ongoing"
									go do_work(&mu, cid, cur_job, jobs, reply.Job_files, reply.Phase)
								}
							}
						} else {
							if cur_status == "done" {
								cur_job = reply.Job_assigned
								if cur_job != nojob { // 任务完成后被分配新任务
									jobs[cur_job] = "ongoing"
									go do_work(&mu, cid, cur_job, jobs, reply.Job_files, reply.Phase)
								}
							} else if cur_status == "idle" {
								cur_job = reply.Job_assigned
								if cur_job != nojob { // 处于 idle 状态的 worker 被分配任务
									jobs[cur_job] = "ongoing"
									go do_work(&mu, cid, cur_job, jobs, reply.Job_files, reply.Phase)
								}
							} else { // ongoing
								if cur_job != reply.Job_assigned {
									log.Fatal("Current job not match the job sent to master")
								}
							}
						}
					}
				}
			}
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

func do_work(mu *sync.Mutex, cid string, job string, jobs map[string]string, job_files []string, phase string) {
	fmt.Println("[" + phase + "]: " + cid + " is running " + job + " with " + strconv.Itoa(len(job_files)) + " files ...")
	time.Sleep(time.Second * 5)
	fmt.Println(cid + " finish " + job)
	mu.Lock()
	jobs[job] = "done"
	mu.Unlock()
}
