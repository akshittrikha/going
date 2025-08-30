package main

import (
	"bufio"
	"os"
	"sync"
	"strings"
	"fmt"
	"sort"
	"time"
)

type KeyValue struct {
	Key string
	Value int
}

// Mapper: takes a line, emits (word, 1)
func mapper(line string, out chan<- KeyValue) {
	words := strings.Fields(line)
	for _, w := range words {
		out <- KeyValue{Key: w, Value: 1}
	}
}

func reducer(key string, values []int) KeyValue {
	sum := 0
	for _, v := range values {
		sum += v
	}
	return KeyValue{Key: key, Value: sum}
}

func main() {
	startTime := time.Now() // Start timing
	file, _ := os.Open("input.txt")
	defer file.Close()

	mapOut := make(chan KeyValue, 100)
	var wg sync.WaitGroup

	// Map phase
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		wg.Add(1)
		go func(l string) {
			defer wg.Done()
			mapper(l, mapOut)
		}(line)
	}

	go func() {
		wg.Wait()
		close(mapOut)
	}()

	// Shuflle phase
	intermediate := make(map[string][]int)
	for kv := range mapOut {
		intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
	}

	// Reduce phase
	results := make([]KeyValue, 0, len(intermediate))
	for k, v := range intermediate {
		results = append(results, reducer(k, v));
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Value > results[j].Value
	})

	for _, res := range results {
    fmt.Printf("%s: -> %d\n", res.Key, res.Value)
}

	elapsedTime := time.Since(startTime)
	// Calculate elapsed time
	fmt.Printf("Execution time: %v\n", elapsedTime) // Print execution time
}