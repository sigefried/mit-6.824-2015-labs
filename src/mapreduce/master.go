package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

type JobStatus struct {
	JobID int
	Ret   bool
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	// handle worker registeration
	go func() {
		for mr.alive {
			addr := <-mr.registerChannel
			mr.avaliableWorkers <- addr
		}
	}()

	sendMapJob := func(jobNum int, syncChan chan JobStatus) {
		addr := <-mr.avaliableWorkers
		ret := mr.SendJobToWorker(addr, Map, mr.nReduce, jobNum)
		if ret {
			mr.avaliableWorkers <- addr
		} else {
			mr.tmpFailedWorkers <- addr
		}
		syncChan <- JobStatus{jobNum, ret}
	}

	sendReduceJob := func(jobNum int, syncChan chan JobStatus) {
		addr := <-mr.avaliableWorkers
		ret := mr.SendJobToWorker(addr, Reduce, mr.nMap, jobNum)
		if ret {
			mr.avaliableWorkers <- addr
		} else {
			mr.tmpFailedWorkers <- addr
		}
		syncChan <- JobStatus{jobNum, ret}
	}

	// send map job
	syncMapChan := make(chan JobStatus, mr.nMap)
	for i := 0; i < mr.nMap; i++ {
		go sendMapJob(i, syncMapChan)
	}

	for i := 0; i < mr.nMap; {
		currentStatus := <-syncMapChan
		if currentStatus.Ret == true {
			i++
		} else {
			go sendMapJob(currentStatus.JobID, syncMapChan)
		}
	}

	// send reduce job
	syncReduceChan := make(chan JobStatus, mr.nReduce)
	for j := 0; j < mr.nReduce; j++ {
		go sendReduceJob(j, syncReduceChan)
	}

	for j := 0; j < mr.nReduce; {
		currentStatus := <-syncReduceChan
		if currentStatus.Ret == true {
			j++
		} else {
			go sendReduceJob(currentStatus.JobID, syncReduceChan)
		}
	}

	return mr.KillWorkers()
}

func (mr *MapReduce) SendJobToWorker(addr string, job JobType, numOtherPhase int, jobNum int) bool {
	args := &DoJobArgs{}
	var reply DoJobReply
	args.File = mr.file
	args.JobNumber = jobNum
	args.NumOtherPhase = numOtherPhase
	args.Operation = job
	ok := call(addr, "Worker.DoJob", args, &reply)
	if ok == false {
		fmt.Printf("DoWork: RPC %s DoJob error\n", mr.MasterAddress)
	}
	if ok && reply.OK {
		return true
	} else {
		return false
	}
}
