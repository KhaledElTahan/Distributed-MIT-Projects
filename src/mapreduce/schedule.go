package mapreduce

import (
	"fmt"
	"sync"
	"math/rand"
)

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
	
	workers := make([]string, 0)
	tasks := make(map[int]DoTaskArgs)

	for i := 0 ; i < ntasks ; i++ {
		var task_args DoTaskArgs

		switch phase{
			case mapPhase:
				task_args = DoTaskArgs{jobName, mapFiles[i], mapPhase, i, n_other}
			case reducePhase:
				task_args = DoTaskArgs{jobName, "", reducePhase, i, n_other}
		}

		tasks[i] = task_args
	}

	var wg sync.WaitGroup	
	for ended := false ; ended != true ; {
		for flag := true ; flag ; {
			select {
			    case worker_address := <-registerChan:
			        workers = append(workers, worker_address)
			    default:
			        flag = false
	    	}
		}

		local_tasks := make([]DoTaskArgs, 0)
		for _, v := range(tasks){
			local_tasks = append(local_tasks,v)
			if( len(local_tasks) == len(workers) ){
				break
			}
		}

		wg.Add(minimum(len(workers), len(local_tasks)))

		//shuffle the workers
		for i := range workers {
			j := rand.Intn(i + 1)
			workers[i], workers[j] = workers[j], workers[i]
		}

		for j := 0 ; j < minimum(len(workers), len(local_tasks)) ; j++{
			
			
			go func(w *sync.WaitGroup, worker string, task_args DoTaskArgs, tasks *map[int]DoTaskArgs) {
				success := call(worker, "Worker.DoTask", task_args, nil)
				if success{
					delete(*tasks,task_args.TaskNumber)
				}
				w.Done()
			}(&wg, workers[j], local_tasks[j], &tasks)

		}
		wg.Wait()

		if len(tasks) == 0 {
			ended = true
		}
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}


func minimum(a int, b int) int {
	if a < b {
		return a 
	}
	return b
}