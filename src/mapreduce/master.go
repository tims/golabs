package mapreduce

import "container/list"
import (
  "fmt"
  "sync"
  "time"
)

type WorkerInfo struct {
  address string
  // You can add definitions here.
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
  fmt.Println("RunMaster")

  // init list of available workers
  // read from the mr.registerChannel
  // and add them to the workers array?

  go func() {
    for {
      var address = <-mr.registerChannel
      var worker WorkerInfo = WorkerInfo{address}
      mr.Workers[address] = &worker
      fmt.Println("Look a new worker!", address)
      mr.AvailableWorkers <- &worker
    }
  }()

  var wg = sync.WaitGroup{}
  for i := 0; i < mr.nMap; i++ {
    go func(i int) {
      worker := <-mr.AvailableWorkers
      args := DoJobArgs{mr.file, Map, i, mr.nReduce}
      reply := DoJobReply{}
      fmt.Println("worker", worker.address)
      call(worker.address, "Worker.DoJob", &args, &reply)
      mr.AvailableWorkers <- worker
      wg.Done()
    }(i)
  }
  fmt.Println(mr.nMap)
  wg.Add(mr.nMap)
  wg.Wait()

  time.Sleep(0 * time.Second)


  for i := 0; i < mr.nReduce; {
    for _, worker := range mr.Workers {
      if i < mr.nReduce {
        args := DoJobArgs{mr.file, Reduce, i, mr.nMap}
        reply := DoJobReply{}
        call(worker.address, "Worker.DoJob", &args, &reply)
        i++;
      }
    }
  }

  return mr.KillWorkers()
}
