该实现的研究，以及和我的实现的一些对比
===========================================

1. 文件命名，该实现中：
      中间文件命名为 mrtmp.${job_name}-${map_task_id}-${reduce_task_id}
      结果文件命名为 mrtmp.${job_name}-res-${reduce_task_id}
   
   我的实现中：
	  中间文件命名为 mr-${map_task_id}-${reduce_task_id}
      结果文件命名为 mr-out-${reduce_task_id}

   重点是，该实现中，job 会有一个自己的名字，这样可以区别不同 job 生成的文件，其他都是一样的。
   
   
2. 最为核心的区别，该实现中：
      master 和 worker 都暴露 RPC 接口，这样 master 可以调用 worker 的接口来分配任务和通知 shutdown
	  
   我的实现中：
      worker 并不暴露 RPC 接口，"唯一"的通讯是向 master 上报当前状态，然后接受 master 返回的指示
   
   
3. 执行 Reduce 任务时，worker 如何获取它所负责的中间文件？该实现中：
      - 会接受 nMap 参数，于是根据本身的 reduce_task_id 来得到所有中间文件名，参见 common_reduce.doReduce 函数
	  - 故此，common_rpc.DoTaskArgs 也就是 master 给 worker 分配 job 的接口参数中，有一个 NumOtherPhase 字段；对于 map job，该字段为 nReduce 数，而对于 reduce job，该字段就是 nMap 数
	  - 同时，common_rpc.DoTaskArgs 中 File 字段仅用于 map job，因为 reduce job 已不需要中间文件名
   
   我的实现中：
      map 和 reduce 阶段所需要的文件都是由 master init 时算好的，并通过 job_files 字段传给 worker
   
   
4. worker 暴露 ShutDown 接口，master 调用接口发送 shutdown 指示，worker 返回其一共执行了多少个 jobs
          暴露 DoTask 接口，用于执行 map/reduce job，什么都不用返回


5. master 暴露 register 接口，worker 调用该接口，上报自己的 RPC 接口地址 (unix domain 或 ip address)

   
6. 每个 worker 可以同时运行多少个 jobs？该实现中：
      - 每个 worker 会启动一个 rpc service，然后每次接受到 master 的请求，就会启动一个协程来处理
	  - 由上，理论上 worker 可以同时运行多个 jobs，而且 worker 维护 parallelism 结构记录同时运行的 jobs 数，该变量可以维护当前同时的 jobs 数以及曾经同时运行的最大 job 数
	  - 然而，实际上，这个 parallelism 变量只是测试时使用。测试时在同一个进程中启动多个 workers 协程，它们通过 parallelism 指针共享相同的变量，故此此时记录的实际上是有多少个同时运行的 workers 数以及曾经同时运行的最大 workers 数，而不再是一个 worker 上同时运行的 jobs 数了
	  - worker 还维护类似的 concurrent 字段，每次运行 job 时 ++，结束时 --；但严格要求其小于 2。
	    故此，一个 worker 确实只能同时运行一个任务
	  - 由于 concurrent 和 parallelism 变量一个是 worker 独有，一个是多个 workers 共享，故此共享的 parallelism 变量单独使用一个自己的锁来控制并发，而不使用 worker 的锁
   
   我的实现中：
      - worker 没有自己的结构体，也不会启动 rpc service，而是就是一个函数进程在运行
	  - 和上面类似的是，worker 进程每次收到 master 的启动 job 请求，就启动一个写成运行 job
	  - 由上，理论上 worker 可以同时运行多个 jobs，但是在实现中限制为同时只能启动一个 job
	  - 这是因为 worker 进程维护 cur_job、cur_status，这些变量都只能绑定一个 job，接到 job 会更新 cur_job 变量为该 job，并把 cur_status 置为 ongoing；只有 idle/failed/done 和断线重连状态下，master 才会给 worker 分配新的 job；ongoing 状态下 master 不会分配 job
   
   
7. 每个 worker 维护一个 nRPC 变量，
      - 指定运行多少个任务之后会自动结束，而不用等待 shutdown 指示;
      - 如果 nRPC == -1，则指不限制任务数，只能由 master shutdown 
      - 逻辑是每次接到 rpc 请求，nRPC -- ，到 0 了就结束 listen，关闭 rpc server
	  - 接到 master 的 ShutDown 后，会强行设置 nRPC = 1，这样处理完 ShutDown 后，nRPC 就会减到 0
	  
   我的实现中：
      目前 worker 本身不会退出，会一直尝试连接 master，直到手动关闭进程
	  (但是，如果想要通过 test-mr.sh 的测试，需要把代码中的 log.Println 换为 log.Panicln
	   也即如果遇到错误或者无法连接 master 时要退出，而不能一直重连，否则 test cases 就测不完了)
   

8. master.stopRPCServer 关闭 RPC server 的方式很有趣
      - 是给自己的 (而非 worker 的) ShutDown RPC 接口发送一个请求
      - 目的是为了避免 master 主线程和 RPC 线程之间的 race condition
	    后面我们会看到，该函数是在一个协程中被调用的，而不是在主线程中
	  - 在对 ShutDown RPC 的处理中，会关闭 master.shutdown channel 并关闭对 rpc server 的 listener
   

9. 对应的，研究一下 master.startRPCServer 启动 RPC server 的代码如下
```
	rpcs := rpc.NewServer()
	rpcs.Register(mr)
	os.Remove(mr.address) // only needed for "unix"
	l, e := net.Listen("unix", mr.address)
	if e != nil {
		log.Fatal("RegstrationServer", mr.address, " error: ", e)
	}
	mr.l = l

	go func() {
	loop:
		for {
			select {
			case <-mr.shutdown:
				break loop
			default:
			}
			conn, err := mr.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				debug("RegistrationServer: accept error", err)
				break
			}
		}
		debug("RegistrationServer: done\n")
	}()
```
      - 监听之后，会启动一个协程来处理 Accept 的逻辑
	  - 在每次 Accept 之前，会首先查看 shutdown channel 中是否有消息，如果有，则直接结束退出逻辑
	  - 如果没有 shutdown 消息，那么就正常进入“阻塞”的 Accept 等待，如果没有请求，就一直等着 Accept
	  - 如有请求被 Accept，启动新的协程来处理请求连接，同时进入下一轮循环，查看 shutdown channel
	  - 注意 Accept 函数是阻塞的，如果一直没有请求，那么其他人即使发送 shutdown channel 也不会退出
	  - 只有有请求发生，进入下一轮循环时，才会再次查看 shutdown channel，此时才能退出逻辑
	  - 在 Accept 阻塞的时候，其他人发送 shutdown channel，那么发送消息的代码也会被阻塞，因为此时 shutdown channel 并没有人来接收，故此发送消息处只能等待，故此最好使用协程来发消息
	  - 最后，#8. 中的 ShutDown 逻辑是通过关闭 listener，导致 Accept failed 来实现结束流程。
	    关闭 shutdown channel 只是一个善后工作，和结束 Accept 流程无关
	  - 事实上，从代码实现上看，其实作者并未在任何地方向 shutdown channel 发送任何消息
   

10. master.killWorkers 用于杀掉所有的 workers
      - 遍历所有的 master.workers，调用它们的 ShutDown 接口，并记录每个 worker 完成的任务数
	  
	  
11. newMaster 用于生成一个 Master 实例，并初始化和具体任务无关的那些成员字段
      - server 地址、shutdown channel，我们知道，shutdown channel 实际上并未真正被使用
	  - doneChannel 这个 channel 用于主线程等待所有任务结束，退出主线程，后面介绍
	  - newCond 条件变量用于 worker 注册之后，通知任务分配机制有新的 worker 到来，后面介绍

  
12. 我们看看整个 Master 的运行逻辑

    主线程首先调用 Distributed(jobName, files, nreduce, master_address) 函数
	   |
	   |-- 调用 newMaster(master_address) 生成一个 Master 实例 mr ，如 #11. 中所示
	   |
	   |-- 调用 mr.startRPCServer()，如 #9. 所示，他会启动 RPC Server，并在成功监听 listen 后，
	   |   启动协程来处理后续 Accept 逻辑和 shutdown channel
	   |
	   |-- 启动协程，运行 mr.run(jobName, files, nreduce, schedule_func, finish_func) 函数
	   |          |
	   |          |-- 根据函数参数，设置 mr.jobName / mr.files / mr.nReduce
	   |          |
	   |          |-- 调用 schedule_func(mapPhase)
	   |          |          |-- 创建一个 channel ch
	   |          |          |-- 再启动一个协程，执行 go mr.forwardRegistrations(ch)
	   |          |          |-- 运行 schedule(jobName, files, nReduce, mapPhase, ch)
	   |          |
	   |          |-- 调用 schedule_func(reducePhase)
	   |          |          |-- 创建一个 channel ch
	   |          |          |-- 再启动一个协程，执行 go mr.forwardRegistrations(ch)
	   |          |          |-- 运行 schedule(jobName, files, nReduce, reducePhase, ch)
	   |          |
	   |          |-- 至此，map & reduce 任务结束，调用 finish_func()
	   |          |          |-- 杀掉所有 workers： mr.stats = mr.killWorkers()，见 #10.
	   |          |          |-- 关闭 master 的 RPC service： mr.stopRPCServer()，见 #8.
	   |          |
	   |          |-- 所有任务完成，把 reduce 文件 merge 到一起
	   |          |
	   |          |-- 最后，向 mr.doneChannel 中发送 true 消息，以结束主线程的等待，终止程序
       |
	   |-- 最终，Distributed 函数运行完毕，返回 Master 实例 mr
	   
	然后，主线程调用 mr.Wait() 函数，该函数就是调用 <-mr.doneChannel 等待 channel 中的 true 消息
	如果没有消息，主线程就会就阻塞等待 (没有 select) 在这个函数上
	一旦等到消息，表示 MapReduce 任务全部结束，整个流程就真正的结束了
   
   
13. master.Register 和 master.forwardRegistrations 的配合

   首先是 master.Register
      - worker 向 master.Register 接口注册时发送 worker 的地址
	  - master 收到 worker 的地址，并添加到 master.workers 列表中
	  - 更重要的是，master 还会发起广播通知 master.newCond.Broadcast()，有什么用？下面马上揭晓
	  
   master.forwardRegistrations
```
	i := 0
	for {
		mr.Lock()
		if len(mr.workers) > i {     // 注意，每次 for loop 只会把一个 worker 加入 channel
			w := mr.workers[i]       // 下次 loop 会再次上锁，并重新对比 workers 和 i
			go func() { ch <- w }()  // 使用协程进行入 channel 操作，以避免阻塞！！！！
			i = i + 1
		} else {
			mr.newCond.Wait()        // 等待新注册 workers 的到来
		}
		mr.Unlock()
	}
```
	  - 无尽循环，每次循环开始首先会上锁，然后检查 master.workers 中的 workers 个数是否等于 i
	  - 如果实际的 workers 个数超过 i，那么
	     + 会陆续的把 workers 的地址通过协程发送到 channel ch 中
		 + 更新 i 值，最终达到 workers 的个数等于 i，此时所有 workers 的地址都已经发到 ch 中了
	  - 如果实际的 workers 个数等于 i，那么
	     + master.newCond.Wait()，这条代码会解锁，然后阻塞住，等待信号发生
		 + 一旦 master.Register 接口被调用，workers 增加数量，并广播信号，上面代码的 Wait 就被激活
		 + Wait 被激活后，会重新上锁，然后完成这个分支的运行，回到开头进行下一轮循环
   总之，该函数会一直阻塞，直到有新的 worker 发起 Register，然后把该 worker 发送到 ch，继续阻塞
   这个函数实现比较巧妙，非常值得学习
   
   那么，最后问题是，这个接收 workers 地址的 channel 有什么用？那么就是下面的 #14. 中要讨论的
   
   
14. Master 的核心逻辑：任务调度 schedule(jobName, mapFiles, nReduce, phase, registerChan) 函数   
      - 该函数的 registerChan 参数就是 #13. 中新 worker 地址被加入的那个 channel
	  - 其他的参数用于构建 DoTask 接口的请求参数
	  - 该函数还会维护 tasks & success 两个新的 channel，前者为待处理任务，后者为处理成功的任务
	  
      - 首先创建 tasks channel，把全部任务 id 入 channel (map 的话就是文件数，reduce 就是 nReduce)
      - 然后创建 success channel，并记录成功任务数为 successTasks := 0
	  - 核心代码如下
```
loop:
	for {
		select {
		case task := <-tasks: 
			go func() {
				worker := <-registerChan
				status := call(worker, "Worker.DoTask", constructTaskArgs(phase, task), nil)
				if status {
					success <- 1
					go func() { registerChan <- worker }()
				} else {
					tasks <- task
				}
			}()
		case <-success:                       // 有成功的任务，那么计数 ++
			successTasks += 1
		default:
			if successTasks == ntasks {       // 如果全部任务结束，那么退出 for loop
				break loop
			}
		}
	}
```
      - 最重心的部分显然是第一个 case，也就是如果有待处理的任务时的处理
	     + 此时会启动一个新的协程，等待空闲 worker 以处理这个任务
		   为什么要启动协程？因为如果 workers 比较少，那么 <-registerChan 这个操作会阻塞
		 + 一旦有了空闲 worker，那么调用该 worker 的 DoTask 接口运行任务，并等待运行结果
		    * 如果任务成功，那么向 success channel 加入新消息，同时把 worker 放回 registerChan
			* 否则，一旦任务失败，把任务 id 重新放回 tasks channel，以便重新调度

   注意，这里有个问题：
      - 如果任务失败，对应 worker 不会被重新放回 registerChan 中，就是说不会再被视为空闲 worker
      - 这里面就有个内在逻辑：如果 DoTask 接口调用失败，那么 master 就认为该 worker 已经挂掉
	  - master 不会再给这个 worker 分配任务，这个 worker 如果还想接受任务，那么必须重新 register
	  - master.workers 是一个数组，故此，如果重新注册，那么 workers 中会有多个该 worker 的地址
	    在 master.killWorkers 的时候，也会被重复关闭，这也是一个隐患，因为会返回完成的任务个数
	  - worker 的实现中，可以遵循这个约定，一旦失败，那么就 log.Fatal 挂掉，可以保证逻辑一致
	    但是，其实还有问题，如果这个 worker 之前运行过一些任务，那么如果挂掉了，这些任务数就丢了
      - 总之，这里会有一些微妙的 bug，不如我的实现效果好
	  

15. 该实现的一些缺陷
      - 由于没有实现 Heartbeat 接口，故此没有处理 10 秒无连接就认为挂掉的逻辑
	  - workers 是有状态的，要维护自己运行的任务数，那么一旦挂掉，master 会丢失信息
	  - 和上面这条联动，master 维护的信息过少，故此无法进行一些数据完整性的维护工作
	  - 实现中使用了很多的 channels，这要求启动很多的协程，以避免工作线程阻塞；
	    好处是省去很多状态相关的遍历，这也进一步使得 master 没有维护很多状态和信息
   
   
   
   
   
   
   
   
   
   
   