package rtcache

import (
	"fmt"
	"sync"

	"github.com/multiversx/mx-chain-core-go/core/check"
	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/types"
)

type unexportedCacher = types.Cacher

type removalTrackingCache struct {
	unexportedCacher
	mutCriticalArea sync.RWMutex
	removalCache    types.Cacher
}

// NewRemovalTrackingCache will create a new instance of a cache that is able to track removal events
func NewRemovalTrackingCache(mainCache types.Cacher, removalCache types.Cacher) (*removalTrackingCache, error) {
	if check.IfNil(mainCache) {
		return nil, fmt.Errorf("%w for the main cache", common.ErrNilCacher)
	}
	if check.IfNil(removalCache) {
		return nil, fmt.Errorf("%w for the removal cache", common.ErrNilCacher)
	}

	return &removalTrackingCache{
		unexportedCacher: mainCache,
		removalCache:     removalCache,
	}, nil
}

// Put adds a value into the main cache. Returns true if an eviction occurred.
// It also removes the key from the removalCache
func (cache *removalTrackingCache) Put(key []byte, value interface{}, sizeInBytes int) (evicted bool) {
	cache.mutCriticalArea.Lock()
	defer cache.mutCriticalArea.Unlock()

	cache.removalCache.Remove(key)
	return cache.unexportedCacher.Put(key, value, sizeInBytes)
}

// Remove removes the provided key from the main cache.
// It also stores the key in the removalCache
func (cache *removalTrackingCache) Remove(key []byte) {
	cache.mutCriticalArea.Lock()
	defer cache.mutCriticalArea.Unlock()

	_ = cache.removalCache.Put(key, struct{}{}, len(key))
	cache.unexportedCacher.Remove(key)
}

// GetRemovalStatus will return the removal status by searching the key in both caches
func (cache *removalTrackingCache) GetRemovalStatus(key []byte) types.RemovalStatus {
	cache.mutCriticalArea.RLock()
	defer cache.mutCriticalArea.RUnlock()

	_, found := cache.removalCache.Get(key)
	if found {
		return types.ExplicitlyRemovedStatus
	}
	_, found = cache.unexportedCacher.Get(key)
	if found {
		return types.NotRemovedStatus
	}

	return types.UnknownRemovalStatus
}

// Clear is used to completely clear both caches.
func (cache *removalTrackingCache) Clear() {
	cache.mutCriticalArea.RLock()
	defer cache.mutCriticalArea.RUnlock()

	cache.removalCache.Clear()
	cache.unexportedCacher.Clear()
}

// IsInterfaceNil returns true if there is no value under the interface
func (cache *removalTrackingCache) IsInterfaceNil() bool {
	return cache == nil
}
