package common

import "time"

// CacheType represents the type of the supported caches
type CacheType string

// Cache types that are currently supported
const (
	LRUCache         CacheType = "LRU"
	SizeLRUCache     CacheType = "SizeLRU"
	FIFOShardedCache CacheType = "FIFOSharded"
)

// DBType represents the type of the supported databases
type DBType string

// DB types that are currently supported
const (
	LvlDB       DBType = "LvlDB"
	LvlDBSerial DBType = "LvlDBSerial"
	MemoryDB    DBType = "MemoryDB"
)

// ShardIDProviderType represents the type for the supported shard id provider
type ShardIDProviderType string

// Shard id provider types that are currently supported
const (
	BinarySplit ShardIDProviderType = "BinarySplit"
)

// MaxRetriesToCreateDB represents the maximum number of times to try to create DB if it failed
const MaxRetriesToCreateDB = 10

// SleepTimeBetweenCreateDBRetries represents the number of seconds to sleep between DB creates
const SleepTimeBetweenCreateDBRetries = 5 * time.Second
