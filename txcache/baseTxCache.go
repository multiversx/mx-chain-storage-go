package txcache

import (
	"sync"

	"github.com/multiversx/mx-chain-core-go/core/check"
	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/types"
)

const maxNumOfEvictionWorkers = 5

type evictionWorkerPool interface {
	Stop()
	Submit(task func())
}

type baseTxCache struct {
	mutEvictionHandlers sync.RWMutex
	evictionHandlers    []types.EvictionNotifier
	evictionWorkerPool  evictionWorkerPool
}

// RegisterEvictionHandler registers a handler which will be called when a tx is evicted from cache
func (cache *baseTxCache) RegisterEvictionHandler(handler types.EvictionNotifier) error {
	if check.IfNil(handler) {
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
	handlers := make([]types.EvictionNotifier, len(cache.evictionHandlers))
	copy(handlers, cache.evictionHandlers)
	cache.mutEvictionHandlers.RUnlock()

	for _, handler := range handlers {
		for _, txHash := range txHashes {
			handler.NotifyEviction(txHash)
		}
	}
}
