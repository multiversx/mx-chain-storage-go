package txcache

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkerPool_WithGoroutinesCheck(t *testing.T) {
	evictedHashes := make(map[string]int)
	mutEvictedHashes := sync.RWMutex{}

	numWorkers := 5
	backgroundGoroutines := runtime.NumGoroutine()
	wp := NewWorkerPool(uint32(numWorkers))
	ctx, cancel := context.WithCancel(context.Background())
	wp.StartWorkingEvictedHashes(ctx, func(hash []byte) {
		mutEvictedHashes.Lock()
		defer mutEvictedHashes.Unlock()

		evictedHashes[string(hash)]++
	})

	// expected goroutines: 5 workers + background
	expectedGoroutines := numWorkers + backgroundGoroutines
	require.Equal(t, expectedGoroutines, runtime.NumGoroutine())

	numHashes := 10000
	for i := 0; i < numHashes; i++ {
		go func(idx int) {
			time.Sleep(time.Millisecond * 100)

			hash := fmt.Sprintf("hash_%d", idx)
			wp.AddEvictedHashes([][]byte{[]byte(hash)})
		}(i)
	}

	// expected goroutines: 10000 AddEvictedHashes + 5 workers + background
	expectedGoroutines = numHashes + numWorkers + backgroundGoroutines
	require.Equal(t, expectedGoroutines, runtime.NumGoroutine())

	// allow all hashes to be "evicted"
	time.Sleep(time.Millisecond * 150)

	// workers still running with no pending evicted tx
	// expected goroutines: 5 workers + background
	expectedGoroutines = numWorkers + backgroundGoroutines
	require.Equal(t, expectedGoroutines, runtime.NumGoroutine())

	// close the workers
	cancel()

	// allow all workers to close
	time.Sleep(time.Millisecond * 5)

	// expected goroutines: background
	expectedGoroutines = backgroundGoroutines
	require.Equal(t, expectedGoroutines, runtime.NumGoroutine())

	mutEvictedHashes.RLock()
	defer mutEvictedHashes.RUnlock()
	require.Equal(t, numHashes, len(evictedHashes))
	for _, cnt := range evictedHashes {
		require.Equal(t, 1, cnt)
	}
}
