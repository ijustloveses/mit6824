package mr

import (
	"errors"
	"fmt"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

/* master 逻辑：
   1. master 维护 worker cid => job 和 job => status 以及 worker cid => status 和 phase 四个变量，还有一把锁
	  其实还有 temp_reduce_jobs 变量，不过这个变量只是用于临时保存 reduce 阶段的 jobs，在 reduce 阶段开始时会赋给 jobs 变量
	  以及 map_jobfiles & reduce_jobfiles 两个变量保存两个阶段任务各自对应的文件列表
   2. 其中 clients (worker cid => job name) 变量维护 workers 正在处理的任务，注意，这里一定是正在处理 ongoing 的任务
   3. 一旦 worker 的任务已经结束 done，那么就会为其分配新的任务，或者空闲不分配任务
   4. jobs 变量维护 master 所有任务的状态 (未分配 = waiting ; 正在处理 = ongoing ; 结束 = done)
   5. conns 变量记录每个 worker 连接的状态，会记录上次 heartbeat 的时间，以及当前是否可用；超过 10 秒没有心跳记为不可用
   6. 断线后，把 worker 的任务重置 waiting 状态，并把该 worker 从 clients 变量中去掉，但不从 conns 中删除（当然也可实现为删除）
   7. worker 断线重连后，效果和首次连接一样，master 会给它分配新的 cid 和任务，而不会继承之前的 cid
   8. clients、jobs 和 conns 三个变量之间的关系和约束见函数 check_inner_status()
   9. 由于本程序在本机运行，中间文件的保存位置都已知，故此不需要记录哪个任务由哪个 worker 完成（中间文件保存在该 worker 上）

   并发逻辑
   1. Heartbeat 接口接受 worker 的状态，并据此来更新内部变量
   2. Done 接口用于汇总当前所有连接的 worker 以及所有任务的状态；如果全部任务都完成，返回 true
   3. check_status 函数用于 a) 检测 workers 的连接状态，处理断线的 workers，也会更新内部变量
                           b) 检测当前是 map 还是 reduce 阶段
   4. 由以上三点，为这三个接口加锁，保证它们按序访问，不会在调用一个接口时被另一个接口更新内部变量
*/

type WorkerStatus struct {
	last_heartbeat time.Time
	is_on          bool
}

type Master struct {
	// Your definitions here.
	mu               sync.Mutex               // for synchronization
	phase            string                   // map / reduce
	clients          map[string]string        // worker cid => job
	jobs             map[string]string        // job -> waiting / ongoing / done
	conns            map[string]*WorkerStatus // worker cid => status
	temp_reduce_jobs map[string]string        // temporarily save reduce jobs
	map_jobfiles     map[string][]string      // job files in map phase
	reduce_jobfiles  map[string][]string      // job file in reduce phase
}

func check_inner_status(clients map[string]string, jobs map[string]string, conns map[string]*WorkerStatus) error {
	/* 检查 clients / jobs / conns 三个变量之间的关系都是正常的 */
	for cid, job := range clients {
		// 保证 clients 中 worker 正在进行的任务一定在 jobs 中，而且其状态一定是 ongoing
		if job != nojob {
			status, matched := jobs[job]
			if !matched {
				return errors.New("Job " + job + " not in master.jobs")
			}
			if status != "ongoing" {
				return errors.New("Job " + job + " has status " + status + " instead of ongoing in master.clients")
			}
		}
		// 保证 worker 一定在 conns 中维护，并且其连接状态一定是 on
		ws, matched := conns[cid]
		if !matched {
			return errors.New("Worker " + cid + " not in master.conns")
		}
		if !ws.is_on {
			return errors.New("Worker " + cid + " is not available in master.conns")
		}
	}
	return nil
}

func (m *Master) init_jobs(files []string, nReduce int) {
	/* 通过 map 阶段的文件名以及 reduce 的个数，初始化所有任务相关的变量 */
	for i, f := range files {
		mj := "m" + strconv.Itoa(i+1)
		m.jobs[mj] = "waiting"           // map job waiting
		m.map_jobfiles[mj] = []string{f} // map job 对应的文件参数只有本文件一个

		for j := 1; j <= nReduce; j++ { // 每个 reduce job 添加该文件对应的 map 阶段生成的中间文件
			rj := "r" + strconv.Itoa(j)
			m.reduce_jobfiles[rj] = append(m.reduce_jobfiles[rj], name_intermediate_file(mj, rj))
		}
	}

	for j := 1; j <= nReduce; j++ {
		rj := "r" + strconv.Itoa(j)
		m.temp_reduce_jobs[rj] = "waiting" // nReduce 个 reduce jobs，都 waiting
	}
}

func (m *Master) init(nReduce int) {
	m.phase = "map"
	files := []string{"foo", "bar", "zoo"}
	m.clients = make(map[string]string)
	m.conns = make(map[string]*WorkerStatus)
	m.jobs = make(map[string]string)
	m.temp_reduce_jobs = make(map[string]string)
	m.map_jobfiles = make(map[string][]string)
	m.reduce_jobfiles = make(map[string][]string)
	m.init_jobs(files, nReduce)
}

// workers 连接的维护，目前的实现中，已断线的 worker 仍然被保留在 m.conns 中未被删除
// 该函数还会检查当前的运行阶段，如果 map 完毕，那么切换到 reduce 阶段
func (m *Master) check_status() {
	m.mu.Lock()
	m.mu.Unlock()

	now := time.Now()
	for cid, ws := range m.conns {
		if ws.is_on { // 只检查那些活跃的 workers，不活跃的会在将来被分配新的 cid
			diff := now.Sub(ws.last_heartbeat)
			if diff.Seconds() > 10 {
				ws.is_on = false
				job := m.clients[cid]
				if job != nojob {
					m.jobs[job] = "waiting" // 重置任务状态
				}
				delete(m.clients, cid) // 删除该 worker
			}
		}
	}

	if m.phase == "map" { // 检查是否 map 阶段已经完毕
		map_done := true
		for _, status := range m.jobs {
			if status != "done" {
				map_done = false
				break
			}
		}
		if map_done { // 进入 reduce 阶段
			m.jobs = m.temp_reduce_jobs
			m.phase = "reduce"
		}
	}
}

func assign_new_cid(conns map[string]*WorkerStatus) string {
	// 由于 conns 不会删除断线的 worker，故此保留了所有的 cid
	// 不使用 clients，因为它会删除断线 worker，当删除了 cid 最大的 worker，再分配时会重复分配最大的 cid
	max_id := 0
	for key := range conns {
		clientid, err := strconv.Atoi(key)
		if err != nil {
			panic(err)
		}
		if clientid > max_id {
			max_id = clientid
		}
	}
	cid := strconv.Itoa(max_id + 1)
	return cid
}

func (m *Master) update_connection(cid string) error {
	/* 之前连接过但是掉线了的非活跃 workers 的原 cid 不会也不应该在此函数中被传入 */
	ws, matched := m.conns[cid]
	if matched { // 之前连接而且当前还活跃的 workers
		if !ws.is_on {
			return errors.New("Worker " + cid + " should be active yet not in master.conns")
		}
		ws.last_heartbeat = time.Now()
	} else { // 首次连接或者断线重连的 workers，此时被分配了新的 cid
		ws = &WorkerStatus{time.Now(), true}
		m.conns[cid] = ws
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
	err := check_inner_status(m.clients, m.jobs, m.conns)
	if err != nil {
		return err
	}

	cid := ""
	need_to_assign_job := false
	reply.Phase = m.phase
	if args.Cid == "0" { // 首次连接，返回最大的 id + 1
		cid = assign_new_cid(m.conns)
		reply.Cid = cid
		need_to_assign_job = true // 等待后面分配任务
		err = m.update_connection(cid)
		if err != nil {
			return err
		}

	} else { // 后续连接，保证 cid 在 m.clients 中，并根据传入的 job 来更新任务状态
		job, matched := m.clients[args.Cid] // m.clients 会删除断线的 worker
		if matched {
			cid = args.Cid
			reply.Cid = cid
			err = m.update_connection(cid)
			if err != nil {
				return err
			}
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
			_, matched := m.conns[args.Cid]
			if matched { // 说明该 worker 断线重连了
				cid = assign_new_cid(m.conns)
				reply.Cid = cid
				need_to_assign_job = true // 等待后面分配任务
				err = m.update_connection(cid)
				if err != nil {
					return err
				}
			} else {
				return errors.New("Failed to match client id")
			}
		}
	}

	// 对新加入的 worker 以及完成了任务的 worker 分配任务
	if need_to_assign_job {
		reply.Job_assigned = nojob
		m.clients[cid] = nojob
		for job, status := range m.jobs {
			if status == "waiting" {
				reply.Job_assigned = job
				if m.phase == "map" {
					reply.Job_files = m.map_jobfiles[job]
				} else {
					reply.Job_files = m.reduce_jobfiles[job]
				}
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

	done := true
	fmt.Println("\n----------------------")
	for job, job_status := range m.jobs {
		fmt.Println(job + ": " + job_status)
		if job_status != "done" {
			done = false
		}
	}
	return done && m.phase == "reduce"
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.init(nReduce)
	go func() {
		for {
			m.check_status()
			time.Sleep(time.Second)
		}
	}()

	m.server()
	return &m
}
