package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type HeartbeatArgs struct {
	Cid        string // 初始时为 "0"
	Cur_job    string // 初始时为 "<NOJOB>"，不是 ""，为了避免 go_rpc_bug.go 中的 bug
	Cur_status string // idle / ongoing / done；注意，当 master 确认 done 之后，进入 idle 状态
}

type HeartbeatReply struct {
	Cid          string
	Job_assigned string
}

const nojob string = "<NOJOB>"

func validate_heartbeat_args(args *HeartbeatArgs) bool {
	if args.Cid == "0" {
		if args.Cur_job == nojob && args.Cur_status == "idle" {
			return true
		} else {
			return false
		}
	} else {
		if args.Cur_job == nojob {
			if args.Cur_status == "idle" {
				return true
			} else {
				return false
			}
		} else {
			if args.Cur_status == "ongoing" || args.Cur_status == "done" {
				return true
			} else {
				return false
			}
		}
	}
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
