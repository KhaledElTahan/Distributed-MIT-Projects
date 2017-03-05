package mapreduce

import (
	"hash/fnv"
	"os"
	"io/ioutil"
	"encoding/json"
)

// doMap manages one map task: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	stream, err := ioutil.ReadFile(inFile)
	check_error(err)

	keyVals := mapF(inFile, string(stream))
	
	results := make(map[int][]KeyValue)
	for _, kv := range keyVals {
		// Calculate R
		r := ihash(kv.Key) % nReduce

		// Map the results internally
		results[r] = append(results[r], kv)
	}

	for r, keyVals := range results {
		outputFileName := reduceName(jobName, mapTaskNumber, r)
		file, err := os.Create(outputFileName)
		check_error(err)
		enc := json.NewEncoder(file)

		for _, kv := range keyVals {
			err := enc.Encode(&kv)
			check_error(err)
		}

		file.Close()
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

func check_error(e error) {
    if e != nil {
        panic(e)
    }
}
