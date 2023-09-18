package txcache

import (
	"sync"

	"github.com/multiversx/mx-chain-storage-go/common"
)

const maxNumOfEvictionWorkers = 5

type evictionWorkerPool interface {
	Stop()
	Submit(task func())
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
func (cache *baseTxCache) notifyEvictionHandlers(txHashes [][]byte) {
	cache.mutEvictionHandlers.RLock()
	handlers := cache.evictionHandlers
	cache.mutEvictionHandlers.RUnlock()

	for _, handler := range handlers {
		for _, txHash := range txHashes {
			handler(txHash)
		}
	}
}
