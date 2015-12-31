package mapreduce

import (
	"container/list"
	"fmt"
	"log"
	"time"
)

type WorkerInfo struct {
	address string

	registerTime    time.Time
	jobDispatchChan chan *JobInfo
	jobFinishChan   chan *JobInfo
}

type JobInfo struct {
	retry int
	DoJobArgs
	DoJobReply
}

func (w *WorkerInfo) work() {
	for {
		select {
		case job := <-w.jobDispatchChan:
			log.Printf("worker %s got job %s", w.address, job.DoJobArgs.String())
			if !call(w.address, "Worker.DoJob", &job.DoJobArgs, &job.DoJobReply) {
				log.Printf("FAILED: worker %s do job %s call error", w.address, job.DoJobArgs.String())
				job.OK = false
			}
			w.jobFinishChan <- job
		}
	}
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

func (mr *MapReduce) dispatch() {
	var jobChan chan *JobInfo
	var job *JobInfo

	for {
		n := len(mr.jobs)
		if n == 0 {
			jobChan = nil
		} else {
			job = mr.jobs[n-1]
			jobChan = mr.jobDispatchChan
		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set jobChan to mr.jobDispatchChan only when there is job to do
		case jobChan <- job:
			mr.jobs = mr.jobs[:n-1]
			mr.jobsDoing++
		case workerAddress := <-mr.registerChannel:
			log.Printf("register a new worker %s", workerAddress)
			worker := &WorkerInfo{
				address:         workerAddress,
				registerTime:    time.Now(),
				jobDispatchChan: mr.jobDispatchChan,
				jobFinishChan:   mr.jobFinishChan,
			}
			mr.Workers[workerAddress] = worker
			go worker.work()
		case job := <-mr.jobFinishChan:
			mr.jobsDoing--
			log.Printf("job[%s] finished", job.DoJobArgs.String())
			if job.OK {
				if mr.jobsDoing == 0 && len(mr.jobs) == 0 {
					if job.Operation == Map {
						log.Println("map jobs are all finished")
						mr.initJobs(Reduce)
					} else {
						return
					}
				}
			} else {
				job.retry++
				mr.jobs = append(mr.jobs, job) // redo job
			}
		}

	}
}

func (mr *MapReduce) initJobs(jobType JobType) {
	var numbers int
	var numberOtherPhase int
	if jobType == Map {
		numbers = mr.nMap
		numberOtherPhase = mr.nReduce
	} else {
		numbers = mr.nReduce
		numberOtherPhase = mr.nMap
	}
	mr.jobs = make([]*JobInfo, numbers)
	for i := 0; i < numbers; i++ {
		mr.jobs[i] = &JobInfo{
			DoJobArgs: DoJobArgs{
				File:          mr.file,
				Operation:     jobType,
				JobNumber:     i,
				NumOtherPhase: numberOtherPhase,
			},
		}
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	mr.initJobs(Map)
	mr.dispatch()
	// Your code here
	return mr.KillWorkers()
}
