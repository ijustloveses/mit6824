package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"errors"
	"os"
	"strconv"
)

/* worker 参数的逻辑：
   1. 每个 worker 未连接 master 之前，Cid 为 “0”。连接 master 之后，由 master 分配 Cid
   2. worker 连接 master 时会汇报当前正在执行的任务名字、该 worker 当前的状态
   3. worker 没有任务时，Cur_job 为 "<NOJOB>"。不设置为 "" 是为了避免 go rpc 的 bug
   4. validate_heartbeat_args 函数的目的是保证 Cid / Cur_job / Cur_status 3 个参数设置是合理的

   master 参数的逻辑：
   1. master 会返回 worker 的 Cid，上面说了这个 Cid 最初就是由 master 分配的，之后保持不变
   2. 还会用 Job_assigned 返回给 worker 分配的任务名字
	  2a) 如果 worker 是初次连接、断线重连、之前没有任务、刚刚完成其他任务的情况，会分配新的任务
	  2b) 如果没有任务分配，那么就设置为 "<NOJOB>"
	  2c) 如果 worker 任务还在进行中，那么这个参数就保持为正在进行中的任务名字
	3. Job_files 含有任务所需的文件列表，而 Phase 会告诉 worker 应该是用 map 还是 reduce 函数
*/

// Add your RPC definitions here.
type HeartbeatArgs struct {
	Cid        string // 初始时为 "0"
	Cur_job    string // 初始时为 "<NOJOB>"，不是 ""，为了避免 go_rpc_bug.go 中的 bug
	Cur_status string // idle / ongoing / done；注意，当 master 确认 done 之后，进入 idle 状态
}

type HeartbeatReply struct {
	Cid          string   // 分配的 Cid
	Job_assigned string   // 分配的任务 id
	Job_files    []string // 任务对应的文件列表，对于 map 任务就只有一个文件；reduce 任务可能有多个
	Phase        string   // map / reduce
}

const nojob string = "<NOJOB>"

func validate_heartbeat_args(args *HeartbeatArgs) bool {
	// 初次连接 master 时，保证 worker 的参数都是初始状态
	if args.Cid == "0" {
		if args.Cur_job == nojob && args.Cur_status == "idle" {
			return true
		} else {
			return false
		}
	} else {
		// 如果此时 worker 没有任务，那么其状态一定是 idle
		if args.Cur_job == nojob {
			if args.Cur_status == "idle" {
				return true
			} else {
				return false
			}
		} else { // 否则，如果有工作，那么 worker 的状态一定是 ongoing 或者 done
			if args.Cur_status == "ongoing" || args.Cur_status == "done" {
				return true
			} else {
				return false
			}
		}
	}
}

func validate_heartbeat_reply(reply *HeartbeatReply) error {
	if reply.Cid == "0" {
		return errors.New("Master didn't assign a valid cid")
	}
	if reply.Phase != "map" && reply.Phase != "reduce" {
		return errors.New("Master returned an invalid phase: " + reply.Phase)
	}
	return nil
}

func name_intermediate_file(mapjob string, reducejob string) string {
	return "mr-" + mapjob + "-" + reducejob
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
