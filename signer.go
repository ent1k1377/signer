package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

type MH struct {
	data string
	th   int
}

func ExecutePipeline(jobs ...job) {
	if len(jobs) == 0 {
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(jobs))

	in := make(chan interface{})

	for _, j := range jobs {
		out := make(chan interface{})
		go func(j job, in, out chan interface{}) {
			j(in, out)
			close(out)
			wg.Done()
		}(j, in, out)

		in = out
	}

	wg.Wait()
}

func SingleHash(in chan interface{}, out chan interface{}) {
	var wg sync.WaitGroup
	th := 0

	for val := range in {
		wg.Add(1)
		go func(val string, th int) {
			var crc32Val string
			var crc32Md5 string
			var md5Val string

			var innerWg sync.WaitGroup
			innerWg.Add(2)

			go func() {
				defer innerWg.Done()
				crc32Val = DataSignerCrc32(val)
			}()

			go func() {
				defer innerWg.Done()
				md5Val = DataSignerMd5(val)
				crc32Md5 = DataSignerCrc32(md5Val)
			}()

			innerWg.Wait()

			data := crc32Val + "~" + crc32Md5
			out <- MH{data, th}
			wg.Done()
		}(strconv.Itoa(val.(int)), th)
		th++
	}

	wg.Wait()
}

func MultiHash(in chan interface{}, out chan interface{}) {
	var wg sync.WaitGroup

	for val := range in {
		wg.Add(1)

		go func(data MH) {
			result := DataSignerCrc32(strconv.Itoa(data.th) + data.data)
			out <- result
			wg.Done()
		}(val.(MH))
	}

	wg.Wait()
}

func CombineResults(in chan interface{}, out chan interface{}) {
	result := make([]string, 0)
	for val := range in {
		data := val.(string)
		result = append(result, data)
	}

	sort.Strings(result)
	out <- strings.Join(result, "_")
}
