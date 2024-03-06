package lrucache

type syncedLRUCache struct {
	*lruCache
}

// NewSyncedLRUCache creates a new LRU cache instance that calls its inner handlers in a synchronized manner
func NewSyncedLRUCache(size int) (*syncedLRUCache, error) {
	c, err := NewCache(size)
	if err != nil {
		return nil, err
	}

	syncedCache := &syncedLRUCache{
		lruCache: c,
	}

	c.callAddedDataHandlers = syncedCache.callAddedDataHandlersSync
	log.Warn("created a new SyncedLRUCache. This is not designed to be used in production mode!")

	return syncedCache, nil
}

func (c *syncedLRUCache) callAddedDataHandlersSync(key []byte, value interface{}) {
	c.mutAddedDataHandlers.RLock()
	for _, handler := range c.mapDataHandlers {
		handler(key, value)
	}
	c.mutAddedDataHandlers.RUnlock()
}

// IsInterfaceNil returns true if there is no value under the interface
func (c *syncedLRUCache) IsInterfaceNil() bool {
	return c == nil
}
