package mr

import (
	"errors"
	"fmt"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
)

/* master 逻辑：
   1. master 维护 worker cid -> job name 和 job name -> status 两个变量，以及一把锁
   2. 其中 clients 变量维护已连接 workers 正在处理的任务，注意，这里一定是正在处理 ongoing 的任务
   3. 一旦 worker 的任务已经结束 done，那么就会为其分配新的任务，或者空闲不分配任务
   4. jobs 变量维护 master 所有任务的状态 (未分配 = waiting ; 正在处理 = ongoing ; 结束 = done)
   5. clients 和 jobs 两个变量之间的关系和约束见函数 check_inner_status()

   6. Heartbeat 接口接受 worker 的状态，并据此来更新内部变量
   7. Done 接口用于汇总当前所有连接的 worker 以及所有任务的状态；如果全部任务都完成，返回 true
   8. 由 6 & 7 ，为这两个接口加锁，保证两个接口按序访问，不会在调用一个接口是被另一个接口更新内部变量
	  其实， Done 接口是只读接口，貌似不加锁问题也不大，不存在并发写入的情况

	缺陷：目前没有实现以下两个功能
	- 如果 worker 长期没有发送 heartbeat，从 clients 中去掉，并重启该 worker 正在处理的任务
	- worker 长期短线后重连时，重新分配 Cid 和任务
*/

type Master struct {
	// Your definitions here.
	mu      sync.Mutex        // for synchronization
	clients map[string]string // clients' job
	jobs    map[string]string // job -> waiting / ongoing / done
}

func check_inner_status(clients map[string]string, jobs map[string]string) error {
	// 保证 clients 中 worker 正在进行的任务一定在 jobs 中，而且其状态一定是 ongoing
	for _, job := range clients {
		if job != nojob {
			status, matched := jobs[job]
			if !matched {
				return errors.New("Job " + job + " not in master.jobs")
			}
			if status != "ongoing" {
				return errors.New("Job " + job + " has status " + status + " instead of ongoing in master.clients")
			}
		}
	}
	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !validate_heartbeat_args(args) {
		return errors.New("invalid heartbeat args")
	}
	err := check_inner_status(m.clients, m.jobs)
	if err != nil {
		return err
	}

	cid := ""
	need_to_assign_job := false
	if args.Cid == "0" { // 首次连接，返回最大的 id + 1
		max_id := 0
		for key := range m.clients {
			clientid, err := strconv.Atoi(key)
			if err != nil {
				panic(err)
			}
			if clientid > max_id {
				max_id = clientid
			}
		}
		cid = strconv.Itoa(max_id + 1)
		reply.Cid = cid
		need_to_assign_job = true // 等待后面分配任务

	} else { // 后续连接，保证 cid 在 m.clients 中，并根据传入的 job 来更新任务状态
		job, matched := m.clients[args.Cid]
		if matched {
			cid = args.Cid
			reply.Cid = cid
			if job == nojob { // 该客户端尚未分配任务
				// 确保和客户端的状态一致
				if args.Cur_job != nojob {
					return errors.New("Job " + args.Cur_job + " in client but not in master")
				}
				need_to_assign_job = true // 等待后面分配任务

			} else {
				// 确保和客户端的状态一致
				if args.Cur_job != job {
					return errors.New("Job " + job + " in master but " + args.Cur_job + " in worker " + cid)
				}
				if args.Cur_status == "done" { // 任务完毕，更新任务状态，并重新分配任务
					m.jobs[job] = "done"
					need_to_assign_job = true // 等待后面分配任务
				} else { // 如果是 ongoing 状态，那么直接返回，所有都保持不变
					reply.Job_assigned = job
					return nil
				}
			}
		} else {
			return errors.New("Failed to match client id")
		}
	}

	// 对新加入的 worker 以及完成了任务的 worker 分配任务
	if need_to_assign_job {
		reply.Job_assigned = nojob
		m.clients[cid] = nojob
		for job, status := range m.jobs {
			if status == "waiting" {
				reply.Job_assigned = job
				m.jobs[job] = "ongoing"
				m.clients[cid] = job
				break
			}
		}
		return nil
	}
	return errors.New("Unexpected way to finish")
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	/*
		sockname := masterSock()
		os.Remove(sockname)
		l, e := net.Listen("unix", sockname)
		if e != nil {
			log.Fatal("listen error:", e)
		}
		go http.Serve(l, nil)
	*/
	go http.ListenAndServe(":1234", nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()

	num_active_client := len(m.clients)
	fmt.Println("Active clients: " + strconv.Itoa(num_active_client))

	done := true
	for job, job_status := range m.jobs {
		fmt.Println(job + ": " + job_status)
		if job_status != "done" {
			done = false
		}
	}
	return done
}

func (m *Master) init() {
	m.jobs = map[string]string{"foo": "waiting", "bar": "waiting", "zoo": "waiting"}
	m.clients = make(map[string]string)
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.init()

	m.server()
	return &m
}
