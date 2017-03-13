package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
		case mapPhase:
			ntasks = len(mapFiles)
			n_other = nReduce
		case reducePhase:
			ntasks = nReduce
			n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	
	workers := make([]string, 0)
	var wg sync.WaitGroup

	for i := 0 ; i < ntasks ; {

		for flag := true ; flag ; {
			select {
			    case worker_address := <-registerChan:
			        workers = append(workers, worker_address)
			    default:
			        flag = false
	    	}
		}

		wg.Add(len(workers))
		for j := 0 ; j < len(workers) ; j++{
			var task_args DoTaskArgs

			switch phase{
				case mapPhase:
					task_args = DoTaskArgs{jobName, mapFiles[i], mapPhase, i, n_other}
				case reducePhase:
					task_args = DoTaskArgs{jobName, "", reducePhase, i, n_other}
			}
			
			go func(w *sync.WaitGroup, j int, workers []string, tasg_args DoTaskArgs) {
				call(workers[j], "Worker.DoTask", task_args, nil)
				w.Done()
			}(&wg, j, workers, task_args)

			i++
		}
		wg.Wait()
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
