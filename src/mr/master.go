package mr

import (
	"errors"
	"fmt"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
)

type Master struct {
	// Your definitions here.
	mu      sync.Mutex        // for synchronization
	clients map[string]string // clients' job
	jobs    map[string]string // job -> waiting / ongoing / done
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !validate_heartbeat_args(args) {
		return errors.New("invalid heartbeat args")
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
		need_to_assign_job = true

	} else { // 后续连接，保证 cid 在 m.clients 中，并根据传入的 job 来更新任务状态
		job, matched := m.clients[args.Cid]
		if matched {
			cid = args.Cid
			reply.Cid = cid
			if job == "" { // 该客户端尚未分配任务
				// 确保和客户端的状态一致
				if args.Cur_job != "" {
					return errors.New("Job " + args.Cur_job + " in client but not in master")
				}
				if args.Cur_status != "idle" {
					return errors.New("Client " + cid + " should be idle but in " + args.Cur_status)
				}
				need_to_assign_job = true

			} else {
				// 确保和客户端的状态一致
				if args.Cur_job != job {
					return errors.New("Job " + job + " in master but " + args.Cur_job + " in worker " + cid)
				}
				job_status, status_matched := m.jobs[job]
				if !status_matched {
					return errors.New("Failed to match job status for " + job + " from master")
				}
				// 此时，master 处 job 的状态应该是 ongoing (否则表示未分配 waiting 或者已完成 done)
				if job_status != "ongoing" {
					return errors.New("Worker " + cid + " send status " + args.Cur_status + " and master job in " + job_status)
				}
				if args.Cur_status == "done" { // 任务完毕，更新任务状态，并重新分配任务
					m.jobs[job] = "done"
					need_to_assign_job = true
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
		reply.Job_assigned = ""
		m.clients[cid] = ""
		for job, status := range m.jobs {
			if status == "waiting" {
				reply.Job_assigned = job
				m.jobs[job] = "ongoing"
				m.clients[cid] = job
				break
			}
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
