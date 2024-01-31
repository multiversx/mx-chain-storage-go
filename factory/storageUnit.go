package factory

import (
	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/storageUnit"
)

// NewStorageUnitFromConf creates a new storage unit from a storage unit config
func NewStorageUnitFromConf(cacheConf common.CacheConfig, dbConf common.DBConfig) (*storageUnit.Unit, error) {
	if dbConf.MaxBatchSize > int(cacheConf.Capacity) {
		return nil, common.ErrCacheSizeIsLowerThanBatchSize
	}

	cache, err := NewCache(cacheConf)
	if err != nil {
		return nil, err
	}

	argDB := ArgDB{
		DBType:            dbConf.Type,
		Path:              dbConf.FilePath,
		BatchDelaySeconds: dbConf.BatchDelaySeconds,
		MaxBatchSize:      dbConf.MaxBatchSize,
		MaxOpenFiles:      dbConf.MaxOpenFiles,
	}
	db, err := NewDB(argDB)
	if err != nil {
		return nil, err
	}

	return storageUnit.NewStorageUnit(cache, db)
}
