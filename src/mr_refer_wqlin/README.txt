��ʵ�ֵ��о����Լ����ҵ�ʵ�ֵ�һЩ�Ա�
===========================================

1. �ļ���������ʵ���У�
      �м��ļ�����Ϊ mrtmp.${job_name}-${map_task_id}-${reduce_task_id}
      ����ļ�����Ϊ mrtmp.${job_name}-res-${reduce_task_id}
   
   �ҵ�ʵ���У�
	  �м��ļ�����Ϊ mr-${map_task_id}-${reduce_task_id}
      ����ļ�����Ϊ mr-out-${reduce_task_id}

   �ص��ǣ���ʵ���У�job ����һ���Լ������֣�������������ͬ job ���ɵ��ļ�����������һ���ġ�
   
   
2. ��Ϊ���ĵ����𣬸�ʵ���У�
      master �� worker ����¶ RPC �ӿڣ����� master ���Ե��� worker �Ľӿ������������֪ͨ shutdown
	  
   �ҵ�ʵ���У�
      worker ������¶ RPC �ӿڣ�"Ψһ"��ͨѶ���� master �ϱ���ǰ״̬��Ȼ����� master ���ص�ָʾ
   
   
3. ִ�� Reduce ����ʱ��worker ��λ�ȡ����������м��ļ�����ʵ���У�
      - ����� nMap ���������Ǹ��ݱ���� reduce_task_id ���õ������м��ļ������μ� common_reduce.doReduce ����
	  - �ʴˣ�common_rpc.DoTaskArgs Ҳ���� master �� worker ���� job �Ľӿڲ����У���һ�� NumOtherPhase �ֶΣ����� map job�����ֶ�Ϊ nReduce ���������� reduce job�����ֶξ��� nMap ��
	  - ͬʱ��common_rpc.DoTaskArgs �� File �ֶν����� map job����Ϊ reduce job �Ѳ���Ҫ�м��ļ���
   
   �ҵ�ʵ���У�
      map �� reduce �׶�����Ҫ���ļ������� master init ʱ��õģ���ͨ�� job_files �ֶδ��� worker
   
   
4. worker ��¶ ShutDown �ӿڣ�master ���ýӿڷ��� shutdown ָʾ��worker ������һ��ִ���˶��ٸ� jobs
          ��¶ DoTask �ӿڣ�����ִ�� map/reduce job��ʲô�����÷���


5. master ��¶ register �ӿڣ�worker ���øýӿڣ��ϱ��Լ��� RPC �ӿڵ�ַ (unix domain �� ip address)

   
6. ÿ�� worker ����ͬʱ���ж��ٸ� jobs����ʵ���У�
      - ÿ�� worker ������һ�� rpc service��Ȼ��ÿ�ν��ܵ� master �����󣬾ͻ�����һ��Э��������
	  - ���ϣ������� worker ����ͬʱ���ж�� jobs������ worker ά�� parallelism �ṹ��¼ͬʱ���е� jobs �����ñ�������ά����ǰͬʱ�� jobs ���Լ�����ͬʱ���е���� job ��
	  - Ȼ����ʵ���ϣ���� parallelism ����ֻ�ǲ���ʱʹ�á�����ʱ��ͬһ��������������� workers Э�̣�����ͨ�� parallelism ָ�빲����ͬ�ı������ʴ˴�ʱ��¼��ʵ�������ж��ٸ�ͬʱ���е� workers ���Լ�����ͬʱ���е���� workers ������������һ�� worker ��ͬʱ���е� jobs ����
	  - worker ��ά�����Ƶ� concurrent �ֶΣ�ÿ������ job ʱ ++������ʱ --�����ϸ�Ҫ����С�� 2��
	    �ʴˣ�һ�� worker ȷʵֻ��ͬʱ����һ������
	  - ���� concurrent �� parallelism ����һ���� worker ���У�һ���Ƕ�� workers �����ʴ˹���� parallelism ��������ʹ��һ���Լ����������Ʋ���������ʹ�� worker ����
   
   �ҵ�ʵ���У�
      - worker û���Լ��Ľṹ�壬Ҳ�������� rpc service�����Ǿ���һ����������������
	  - ���������Ƶ��ǣ�worker ����ÿ���յ� master ������ job ���󣬾�����һ��д������ job
	  - ���ϣ������� worker ����ͬʱ���ж�� jobs��������ʵ��������Ϊͬʱֻ������һ�� job
	  - ������Ϊ worker ����ά�� cur_job��cur_status����Щ������ֻ�ܰ�һ�� job���ӵ� job ����� cur_job ����Ϊ�� job������ cur_status ��Ϊ ongoing��ֻ�� idle/failed/done �Ͷ�������״̬�£�master �Ż�� worker �����µ� job��ongoing ״̬�� master ������� job
   
   
7. ÿ�� worker ά��һ�� nRPC ������
      - ָ�����ж��ٸ�����֮����Զ������������õȴ� shutdown ָʾ;
      - ��� nRPC == -1����ָ��������������ֻ���� master shutdown 
      - �߼���ÿ�νӵ� rpc ����nRPC -- ���� 0 �˾ͽ��� listen���ر� rpc server
	  - �ӵ� master �� ShutDown �󣬻�ǿ������ nRPC = 1������������ ShutDown ��nRPC �ͻ���� 0
	  
   �ҵ�ʵ���У�
      Ŀǰ worker �������˳�����һֱ�������� master��ֱ���ֶ��رս���
	  (���ǣ������Ҫͨ�� test-mr.sh �Ĳ��ԣ���Ҫ�Ѵ����е� log.Println ��Ϊ log.Panicln
	   Ҳ�����������������޷����� master ʱҪ�˳���������һֱ���������� test cases �Ͳⲻ����)
   

8. master.stopRPCServer �ر� RPC server �ķ�ʽ����Ȥ
      - �Ǹ��Լ��� (���� worker ��) ShutDown RPC �ӿڷ���һ������
      - Ŀ����Ϊ�˱��� master ���̺߳� RPC �߳�֮��� race condition
	    �������ǻῴ�����ú�������һ��Э���б����õģ������������߳���
	  - �ڶ� ShutDown RPC �Ĵ����У���ر� master.shutdown channel ���رն� rpc server �� listener
   

9. ��Ӧ�ģ��о�һ�� master.startRPCServer ���� RPC server �Ĵ�������
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
      - ����֮�󣬻�����һ��Э�������� Accept ���߼�
	  - ��ÿ�� Accept ֮ǰ�������Ȳ鿴 shutdown channel ���Ƿ�����Ϣ������У���ֱ�ӽ����˳��߼�
	  - ���û�� shutdown ��Ϣ����ô���������롰�������� Accept �ȴ������û�����󣬾�һֱ���� Accept
	  - �������� Accept�������µ�Э���������������ӣ�ͬʱ������һ��ѭ�����鿴 shutdown channel
	  - ע�� Accept �����������ģ����һֱû��������ô�����˼�ʹ���� shutdown channel Ҳ�����˳�
	  - ֻ������������������һ��ѭ��ʱ���Ż��ٴβ鿴 shutdown channel����ʱ�����˳��߼�
	  - �� Accept ������ʱ�������˷��� shutdown channel����ô������Ϣ�Ĵ���Ҳ�ᱻ��������Ϊ��ʱ shutdown channel ��û���������գ��ʴ˷�����Ϣ��ֻ�ܵȴ����ʴ����ʹ��Э��������Ϣ
	  - ���#8. �е� ShutDown �߼���ͨ���ر� listener������ Accept failed ��ʵ�ֽ������̡�
	    �ر� shutdown channel ֻ��һ���ƺ������ͽ��� Accept �����޹�
	  - ��ʵ�ϣ��Ӵ���ʵ���Ͽ�����ʵ���߲�δ���κεط��� shutdown channel �����κ���Ϣ
   

10. master.killWorkers ����ɱ�����е� workers
      - �������е� master.workers���������ǵ� ShutDown �ӿڣ�����¼ÿ�� worker ��ɵ�������
	  
	  
11. newMaster ��������һ�� Master ʵ��������ʼ���;��������޹ص���Щ��Ա�ֶ�
      - server ��ַ��shutdown channel������֪����shutdown channel ʵ���ϲ�δ������ʹ��
	  - doneChannel ��� channel �������̵߳ȴ���������������˳����̣߳��������
	  - newCond ������������ worker ע��֮��֪ͨ�������������µ� worker �������������

  
12. ���ǿ������� Master �������߼�

    ���߳����ȵ��� Distributed(jobName, files, nreduce, master_address) ����
	   |
	   |-- ���� newMaster(master_address) ����һ�� Master ʵ�� mr ���� #11. ����ʾ
	   |
	   |-- ���� mr.startRPCServer()���� #9. ��ʾ���������� RPC Server�����ڳɹ����� listen ��
	   |   ����Э����������� Accept �߼��� shutdown channel
	   |
	   |-- ����Э�̣����� mr.run(jobName, files, nreduce, schedule_func, finish_func) ����
	   |          |
	   |          |-- ���ݺ������������� mr.jobName / mr.files / mr.nReduce
	   |          |
	   |          |-- ���� schedule_func(mapPhase)
	   |          |          |-- ����һ�� channel ch
	   |          |          |-- ������һ��Э�̣�ִ�� go mr.forwardRegistrations(ch)
	   |          |          |-- ���� schedule(jobName, files, nReduce, mapPhase, ch)
	   |          |
	   |          |-- ���� schedule_func(reducePhase)
	   |          |          |-- ����һ�� channel ch
	   |          |          |-- ������һ��Э�̣�ִ�� go mr.forwardRegistrations(ch)
	   |          |          |-- ���� schedule(jobName, files, nReduce, reducePhase, ch)
	   |          |
	   |          |-- ���ˣ�map & reduce ������������� finish_func()
	   |          |          |-- ɱ������ workers�� mr.stats = mr.killWorkers()���� #10.
	   |          |          |-- �ر� master �� RPC service�� mr.stopRPCServer()���� #8.
	   |          |
	   |          |-- ����������ɣ��� reduce �ļ� merge ��һ��
	   |          |
	   |          |-- ����� mr.doneChannel �з��� true ��Ϣ���Խ������̵߳ĵȴ�����ֹ����
       |
	   |-- ���գ�Distributed ����������ϣ����� Master ʵ�� mr
	   
	Ȼ�����̵߳��� mr.Wait() �������ú������ǵ��� <-mr.doneChannel �ȴ� channel �е� true ��Ϣ
	���û����Ϣ�����߳̾ͻ�������ȴ� (û�� select) �����������
	һ���ȵ���Ϣ����ʾ MapReduce ����ȫ���������������̾������Ľ�����
   
   
13. master.Register �� master.forwardRegistrations �����

   ������ master.Register
      - worker �� master.Register �ӿ�ע��ʱ���� worker �ĵ�ַ
	  - master �յ� worker �ĵ�ַ������ӵ� master.workers �б���
	  - ����Ҫ���ǣ�master ���ᷢ��㲥֪ͨ master.newCond.Broadcast()����ʲô�ã��������Ͻ���
	  
   master.forwardRegistrations
```
	i := 0
	for {
		mr.Lock()
		if len(mr.workers) > i {     // ע�⣬ÿ�� for loop ֻ���һ�� worker ���� channel
			w := mr.workers[i]       // �´� loop ���ٴ������������¶Ա� workers �� i
			go func() { ch <- w }()  // ʹ��Э�̽����� channel �������Ա���������������
			i = i + 1
		} else {
			mr.newCond.Wait()        // �ȴ���ע�� workers �ĵ���
		}
		mr.Unlock()
	}
```
	  - �޾�ѭ����ÿ��ѭ����ʼ���Ȼ�������Ȼ���� master.workers �е� workers �����Ƿ���� i
	  - ���ʵ�ʵ� workers �������� i����ô
	     + ��½���İ� workers �ĵ�ַͨ��Э�̷��͵� channel ch ��
		 + ���� i ֵ�����մﵽ workers �ĸ������� i����ʱ���� workers �ĵ�ַ���Ѿ����� ch ����
	  - ���ʵ�ʵ� workers �������� i����ô
	     + master.newCond.Wait()����������������Ȼ������ס���ȴ��źŷ���
		 + һ�� master.Register �ӿڱ����ã�workers �������������㲥�źţ��������� Wait �ͱ�����
		 + Wait ������󣬻�����������Ȼ����������֧�����У��ص���ͷ������һ��ѭ��
   ��֮���ú�����һֱ������ֱ�����µ� worker ���� Register��Ȼ��Ѹ� worker ���͵� ch����������
   �������ʵ�ֱȽ�����ǳ�ֵ��ѧϰ
   
   ��ô����������ǣ�������� workers ��ַ�� channel ��ʲô�ã���ô��������� #14. ��Ҫ���۵�
   
   
14. Master �ĺ����߼���������� schedule(jobName, mapFiles, nReduce, phase, registerChan) ����   
      - �ú����� registerChan �������� #13. ���� worker ��ַ��������Ǹ� channel
	  - �����Ĳ������ڹ��� DoTask �ӿڵ��������
	  - �ú�������ά�� tasks & success �����µ� channel��ǰ��Ϊ���������񣬺���Ϊ����ɹ�������
	  
      - ���ȴ��� tasks channel����ȫ������ id �� channel (map �Ļ������ļ�����reduce ���� nReduce)
      - Ȼ�󴴽� success channel������¼�ɹ�������Ϊ successTasks := 0
	  - ���Ĵ�������
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
		case <-success:                       // �гɹ���������ô���� ++
			successTasks += 1
		default:
			if successTasks == ntasks {       // ���ȫ�������������ô�˳� for loop
				break loop
			}
		}
	}
```
      - �����ĵĲ�����Ȼ�ǵ�һ�� case��Ҳ��������д����������ʱ�Ĵ���
	     + ��ʱ������һ���µ�Э�̣��ȴ����� worker �Դ����������
		   ΪʲôҪ����Э�̣���Ϊ��� workers �Ƚ��٣���ô <-registerChan �������������
		 + һ�����˿��� worker����ô���ø� worker �� DoTask �ӿ��������񣬲��ȴ����н��
		    * �������ɹ�����ô�� success channel ��������Ϣ��ͬʱ�� worker �Ż� registerChan
			* ����һ������ʧ�ܣ������� id ���·Ż� tasks channel���Ա����µ���

   ע�⣬�����и����⣺
      - �������ʧ�ܣ���Ӧ worker ���ᱻ���·Ż� registerChan �У�����˵�����ٱ���Ϊ���� worker
      - ��������и������߼������ DoTask �ӿڵ���ʧ�ܣ���ô master ����Ϊ�� worker �Ѿ��ҵ�
	  - master �����ٸ���� worker ����������� worker ����������������ô�������� register
	  - master.workers ��һ�����飬�ʴˣ��������ע�ᣬ��ô workers �л��ж���� worker �ĵ�ַ
	    �� master.killWorkers ��ʱ��Ҳ�ᱻ�ظ��رգ���Ҳ��һ����������Ϊ�᷵����ɵ��������
	  - worker ��ʵ���У�������ѭ���Լ����һ��ʧ�ܣ���ô�� log.Fatal �ҵ������Ա�֤�߼�һ��
	    ���ǣ���ʵ�������⣬������ worker ֮ǰ���й�һЩ������ô����ҵ��ˣ���Щ�������Ͷ���
      - ��֮���������һЩ΢��� bug�������ҵ�ʵ��Ч����
	  

15. ��ʵ�ֵ�һЩȱ��
      - ����û��ʵ�� Heartbeat �ӿڣ��ʴ�û�д��� 10 �������Ӿ���Ϊ�ҵ����߼�
	  - workers ����״̬�ģ�Ҫά���Լ����е�����������ôһ���ҵ���master �ᶪʧ��Ϣ
	  - ����������������master ά������Ϣ���٣��ʴ��޷�����һЩ���������Ե�ά������
	  - ʵ����ʹ���˺ܶ�� channels����Ҫ�������ܶ��Э�̣��Ա��⹤���߳�������
	    �ô���ʡȥ�ܶ�״̬��صı�������Ҳ��һ��ʹ�� master û��ά���ܶ�״̬����Ϣ
   
   
   
   
   
   
   
   
   
   
   