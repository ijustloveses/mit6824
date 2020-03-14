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

type Master struct {
	// Your definitions here.
	Is_Done bool
	mu      sync.Mutex
	clients map[int]bool
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Heartbeat(arg int, reply *int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if arg == 0 { // 首次连接，返回最大的 id + 1
		max_id := 0
		for key := range m.clients {
			if key > max_id {
				max_id = key
			}
		}
		*reply = max_id + 1
		m.clients[max_id+1] = true
		return nil
	} else { // 后续连接，传入的 arg 为该 client 的 id，需要保证 id 在 m.clients 中
		for key := range m.clients {
			if key == arg {
				*reply = key
				m.clients[key] = true
				return nil
			}
		}
		*reply = arg
		return errors.New("Failed to match client id")
	}
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
	m.Is_Done = false
	m.clients = make(map[int]bool)
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

	go func() {
		time.Sleep(time.Second * 30)
		m.Is_Done = true
	}()
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()

	num_active := 0
	for _, is_active := range m.clients {
		if is_active {
			num_active += 1
		}
	}
	fmt.Println("Active clients: " + strconv.Itoa(num_active))
	ret = m.Is_Done

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.server()
	return &m
}
