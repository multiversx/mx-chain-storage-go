package lrucache

import (
	"github.com/multiversx/mx-chain-storage-go/types"
)

// simpleLRUCacheAdapter provides an adapter between LRUCacheHandler and SizeLRUCacheHandler
type simpleLRUCacheAdapter struct {
	types.LRUCacheHandler
}

// AddSized calls the Add method without the size in bytes parameter
func (slca *simpleLRUCacheAdapter) AddSized(key, value interface{}, _ int64) bool {
	return slca.Add(key, value)
}

// AddSizedIfMissing calls ContainsOrAdd without the size in bytes parameter
func (slca *simpleLRUCacheAdapter) AddSizedIfMissing(key, value interface{}, _ int64) (ok, evicted bool) {
	return slca.ContainsOrAdd(key, value)
}

// SizeInBytesContained returns 0
func (slca *simpleLRUCacheAdapter) SizeInBytesContained() uint64 {
	return 0
}
