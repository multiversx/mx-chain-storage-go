package common

import (
	"errors"
)

// ErrNilPersister is raised when a nil persister is provided
var ErrNilPersister = errors.New("expected not nil persister")

// ErrNilCacher is raised when a nil cacher is provided
var ErrNilCacher = errors.New("expected not nil cacher")

// ErrNotSupportedCacheType is raised when an unsupported cache type is provided
var ErrNotSupportedCacheType = errors.New("not supported cache type")

// ErrNotSupportedDBType is raised when an unsupported database type is provided
var ErrNotSupportedDBType = errors.New("not supported db type")

// ErrNotSupportedHashType is raised when an unsupported hasher is provided
var ErrNotSupportedHashType = errors.New("hash type not supported")

// ErrKeyNotFound is raised when a key is not found
var ErrKeyNotFound = errors.New("key not found")

// ErrInvalidBatch is raised when the used batch is invalid
var ErrInvalidBatch = errors.New("batch is invalid")

// ErrInvalidNumOpenFiles is raised when the max num of open files is less than 1
var ErrInvalidNumOpenFiles = errors.New("maxOpenFiles is invalid")

// ErrEmptyKey is raised when a key is empty
var ErrEmptyKey = errors.New("key is empty")

// ErrInvalidConfig signals an invalid config
var ErrInvalidConfig = errors.New("invalid config")

// ErrOldestEpochNotAvailable signals that fetching the oldest epoch is not available
var ErrOldestEpochNotAvailable = errors.New("oldest epoch not available")

// ErrCacheSizeIsLowerThanBatchSize signals that size of cache is lower than size of batch
var ErrCacheSizeIsLowerThanBatchSize = errors.New("cache size is lower than batch size")

// ErrNilMarshalizer signals that a nil marshalizer has been provided
var ErrNilMarshalizer = errors.New("nil marshalizer")

// ErrFailedCacheEviction signals a failed eviction within a cache
var ErrFailedCacheEviction = errors.New("failed eviction within cache")

// ErrImmuneItemsCapacityReached signals that capacity for immune items is reached
var ErrImmuneItemsCapacityReached = errors.New("capacity reached for immune items")

// ErrItemAlreadyInCache signals that an item is already in cache
var ErrItemAlreadyInCache = errors.New("item already in cache")

// ErrCacheSizeInvalid signals that size of cache is less than 1
var ErrCacheSizeInvalid = errors.New("cache size is less than 1")

// ErrCacheCapacityInvalid signals that capacity of cache is less than 1
var ErrCacheCapacityInvalid = errors.New("cache capacity is less than 1")

// ErrLRUCacheWithProvidedSize signals that a simple LRU cache is wanted but the user provided a positive size in bytes value
var ErrLRUCacheWithProvidedSize = errors.New("LRU cache does not support size in bytes")

// ErrLRUCacheInvalidSize signals that the provided size in bytes value for LRU cache is invalid
var ErrLRUCacheInvalidSize = errors.New("wrong size in bytes value for LRU cache")

// ErrNegativeSizeInBytes signals that the provided size in bytes value is negative
var ErrNegativeSizeInBytes = errors.New("negative size in bytes")

// ErrNilTimeCache signals that a nil time cache has been provided
var ErrNilTimeCache = errors.New("nil time cache")

// ErrNilTxGasHandler signals that a nil tx gas handler was provided
var ErrNilTxGasHandler = errors.New("nil tx gas handler")

// ErrNilStoredDataFactory signals that a nil stored data factory has been provided
var ErrNilStoredDataFactory = errors.New("nil stored data factory")

// ErrInvalidDefaultSpan signals that an invalid default span was provided
var ErrInvalidDefaultSpan = errors.New("invalid default span")

// ErrInvalidCacheExpiry signals that an invalid cache expiry was provided
var ErrInvalidCacheExpiry = errors.New("invalid cache expiry")

// ErrDBIsClosed is raised when the DB is closed
var ErrDBIsClosed = errors.New("DB is closed")
