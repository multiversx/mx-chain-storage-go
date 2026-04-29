package common

import (
	"encoding/json"

	logger "github.com/multiversx/mx-chain-logger-go"
)

var log = logger.GetOrCreate("common")

// CacheConfig holds the configurable elements of a cache
type CacheConfig struct {
	Name                 string
	Type                 CacheType
	SizeInBytes          uint64
	SizeInBytesPerSender uint32
	Capacity             uint32
	SizePerSender        uint32
	Shards               uint32
}

// String returns a readable representation of the object
func (config *CacheConfig) String() string {
	bytes, err := json.Marshal(config)
	if err != nil {
		log.Error("CacheConfig.String()", "err", err)
	}

	return string(bytes)
}

// DBConfig holds the configurable elements of a database
type DBConfig struct {
	FilePath          string
	Type              DBType
	BatchDelaySeconds int
	MaxBatchSize      int
	MaxOpenFiles      int
	// BloomFilterBtsPerKey == 0, the Bloom filter is disabled.
	// Otherwise, it specifies the number of bits per key used by the Bloom filter.
	BloomFilterBtsPerKey int
}
