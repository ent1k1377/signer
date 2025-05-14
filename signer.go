package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

const HashWorkerCount = 6

func ExecutePipeline(jobs ...job) {
	if len(jobs) == 0 {
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(jobs))

	in := make(chan any)

	for _, j := range jobs {
		out := make(chan any)
		go func(j job, in, out chan any) {
			j(in, out)
			close(out)
			wg.Done()
		}(j, in, out)

		in = out
	}

	wg.Wait()
}

func SingleHash(in chan any, out chan any) {
	var wg sync.WaitGroup
	var md5Mutex sync.Mutex

	for data := range in {
		wg.Add(1)
		dataStr := strconv.Itoa(data.(int))

		go func(inputData string) {
			defer wg.Done()

			var crc32Result, crc32Md5Result string

			var innerWg sync.WaitGroup
			innerWg.Add(2)

			go func() {
				defer innerWg.Done()
				crc32Result = DataSignerCrc32(inputData)
			}()

			go func() {
				defer innerWg.Done()

				md5Mutex.Lock()
				md5Val := DataSignerMd5(inputData)
				md5Mutex.Unlock()

				crc32Md5Result = DataSignerCrc32(md5Val)
			}()

			innerWg.Wait()

			res := crc32Result + "~" + crc32Md5Result
			out <- res
		}(dataStr)
	}

	wg.Wait()
}

func MultiHash(in chan any, out chan any) {
	var wg sync.WaitGroup
	var mux sync.Mutex

	for data := range in {
		wg.Add(1)
		dataStr := data.(string)

		go func(inputData string) {
			defer wg.Done()

			var innerWg sync.WaitGroup
			hashes := make([]string, HashWorkerCount)

			for th := 0; th < HashWorkerCount; th++ {
				innerWg.Add(1)
				go func(th int) {
					hash := DataSignerCrc32(strconv.Itoa(th) + inputData)
					mux.Lock()
					defer mux.Unlock()
					hashes[th] = hash
					innerWg.Done()
				}(th)
			}

			innerWg.Wait()

			var result string
			for th := 0; th < HashWorkerCount; th++ {
				result += hashes[th]
			}

			out <- result
		}(dataStr)
	}

	wg.Wait()
}

func CombineResults(in chan any, out chan any) {
	result := make([]string, 0)

	for data := range in {
		dataStr := data.(string)
		result = append(result, dataStr)
	}

	sort.Strings(result)
	out <- strings.Join(result, "_")
}
