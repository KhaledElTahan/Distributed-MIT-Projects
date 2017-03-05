package mapreduce

import (
	"os"
	"encoding/json"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	key_vals := make(map[string][]string)
	for m := 0 ; m < nMap ; m++ {
		intermediateFileName := reduceName(jobName, m, reduceTaskNumber) 
		file, err := os.Open(intermediateFileName)
		check_error(err)

		dec := json.NewDecoder(file)

		var kv KeyValue
		for  ; true ;  {
			err := dec.Decode(&kv)

			if err != nil{
				break 
			}

			key_vals[kv.Key] = append(key_vals[kv.Key], kv.Value)
		}

		file.Close()
	}

	file, err := os.Create(outFile)
	check_error(err)
	enc := json.NewEncoder(file)

	for key, values := range key_vals {
		sort.Strings(values)
		enc.Encode(KeyValue{key, reduceF(key, values)})
	}

	file.Close()
}
