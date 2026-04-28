package factory

import (
	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/leveldb"
	"github.com/multiversx/mx-chain-storage-go/memorydb"
	"github.com/multiversx/mx-chain-storage-go/types"
)

// ArgDB is a structure that is used to create a new storage.Persister implementation
type ArgDB struct {
	DBType            common.DBType
	Path              string
	BatchDelaySeconds int
	MaxBatchSize      int
	MaxOpenFiles      int
	// BloomFilterBtsPerKey == 0, the Bloom filter is disabled.
	// Otherwise, it specifies the number of bits per key used by the Bloom filter.
	BloomFilterBtsPerKey int
}

// NewDB creates a new database from database config
func NewDB(argDB ArgDB) (types.Persister, error) {
	switch argDB.DBType {
	case common.LvlDB:
		return leveldb.NewDB(argDB.Path, argDB.BatchDelaySeconds, argDB.MaxBatchSize, argDB.MaxOpenFiles, argDB.BloomFilterBtsPerKey)
	case common.LvlDBSerial:
		return leveldb.NewSerialDB(argDB.Path, argDB.BatchDelaySeconds, argDB.MaxBatchSize, argDB.MaxOpenFiles, argDB.BloomFilterBtsPerKey)
	case common.MemoryDB:
		return memorydb.New(), nil
	default:
		return nil, common.ErrNotSupportedDBType
	}
}
