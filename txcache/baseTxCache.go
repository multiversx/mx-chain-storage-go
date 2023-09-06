package txcache

import (
	"context"
	"sync"

	"github.com/multiversx/mx-chain-storage-go/common"
)

const numOfEvictionWorkers = uint32(5)

type evictionWorkerPool interface {
	StartWorkingEvictedHashes(ctx context.Context, handler func([]byte))
	AddEvictedHashes(hashes [][]byte)
}

type baseTxCache struct {
	mutEvictionHandlers sync.RWMutex
	evictionHandlers    []func(txHash []byte)
	evictionWorkerPool  evictionWorkerPool
}

// RegisterEvictionHandler registers a handler which will be called when a tx is evicted from cache
func (cache *baseTxCache) RegisterEvictionHandler(handler func(hash []byte)) error {
	if handler == nil {
		return common.ErrNilEvictionHandler
	}

	cache.mutEvictionHandlers.Lock()
	cache.evictionHandlers = append(cache.evictionHandlers, handler)
	cache.mutEvictionHandlers.Unlock()

	return nil
}

// notifyEvictionHandlers will be called on a separate go routine
func (cache *baseTxCache) notifyEvictionHandlers(txHash []byte) {
	cache.mutEvictionHandlers.RLock()
	for _, handler := range cache.evictionHandlers {
		handler(txHash)
	}
	cache.mutEvictionHandlers.RUnlock()
}
