package pebbledb

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/cockroachdb/pebble/v2"

	"github.com/multiversx/mx-chain-storage-go/common"
)

// SharedResources holds the block cache and the file cache shared by all pebble persisters of a process
type SharedResources struct {
	// the block cache type lives in a pebble internal package, so an Options value is the only way to hold it
	cacheHolder pebble.Options
	fileCache   *pebble.FileCache
	closeOnce   sync.Once
}

// NewSharedResources creates the shared caches; Close them only after the last persister using them is closed
func NewSharedResources(blockCacheSizeBytes int64, fileCacheSize int) (*SharedResources, error) {
	if blockCacheSizeBytes <= 0 {
		return nil, fmt.Errorf("%w: block cache size must be positive", common.ErrInvalidConfig)
	}
	if fileCacheSize <= 0 {
		return nil, fmt.Errorf("%w: file cache size must be positive", common.ErrInvalidConfig)
	}

	return &SharedResources{
		cacheHolder: pebble.Options{Cache: pebble.NewCache(blockCacheSizeBytes)},
		fileCache:   pebble.NewFileCache(runtime.GOMAXPROCS(0), fileCacheSize),
	}, nil
}

// Close releases the creator's references; the caches are freed once every persister released its own
func (r *SharedResources) Close() {
	if r == nil {
		return
	}

	r.closeOnce.Do(func() {
		r.cacheHolder.Cache.Unref()
		r.fileCache.Unref()
	})
}

// IsInterfaceNil returns true if there is no value under the interface
func (r *SharedResources) IsInterfaceNil() bool {
	return r == nil
}
