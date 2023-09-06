package txcache

import (
	"context"
)

type txCacheEvictionWorkerPool struct {
	maxWorkers         uint32
	evictedHashesQueue chan []byte
}

// NewWorkerPool returns a new workerPool instance
func NewWorkerPool(maxWorkers uint32) *txCacheEvictionWorkerPool {
	return &txCacheEvictionWorkerPool{
		maxWorkers:         maxWorkers,
		evictedHashesQueue: make(chan []byte),
	}
}

// StartWorkingEvictedHashes starts the workers go routines
func (wp *txCacheEvictionWorkerPool) StartWorkingEvictedHashes(ctx context.Context, handler func(hash []byte)) {
	if handler == nil {
		return
	}

	for i := uint32(0); i < wp.maxWorkers; i++ {
		go wp.startWorker(ctx, handler)
	}
}

func (wp *txCacheEvictionWorkerPool) startWorker(ctx context.Context, handler func(hash []byte)) {
	for {
		select {
		case <-ctx.Done():
			log.Debug("closing evicted hashes worker...")
			return
		case evictedHash := <-wp.evictedHashesQueue:
			handler(evictedHash)
		}
	}
}

// AddEvictedHashes adds the evicted hashes to the queue
func (wp *txCacheEvictionWorkerPool) AddEvictedHashes(hashes [][]byte) {
	for i := 0; i < len(hashes); i++ {
		wp.evictedHashesQueue <- hashes[i]
	}
}
