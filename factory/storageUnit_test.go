package factory_test

import (
	"testing"

	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/factory"
	"github.com/stretchr/testify/assert"
)

func TestNewStorageUnitFromConf_WrongCacheSizeVsBatchSize(t *testing.T) {
	t.Parallel()

	storer, err := factory.NewStorageUnitFromConf(common.CacheConfig{
		Capacity: 10,
		Type:     common.LRUCache,
	}, common.DBConfig{
		FilePath:          "Blocks",
		Type:              common.LvlDB,
		MaxBatchSize:      11,
		BatchDelaySeconds: 1,
		MaxOpenFiles:      10,
	},
	)

	assert.NotNil(t, err, "error expected")
	assert.Nil(t, storer, "storer expected to be nil but got %s", storer)
}

func TestNewStorageUnitFromConf_WrongCacheConfig(t *testing.T) {
	t.Parallel()

	storer, err := factory.NewStorageUnitFromConf(common.CacheConfig{
		Capacity: 10,
		Type:     "NotLRU",
	}, common.DBConfig{
		FilePath:          "Blocks",
		Type:              common.LvlDB,
		BatchDelaySeconds: 1,
		MaxBatchSize:      1,
		MaxOpenFiles:      10,
	},
	)

	assert.NotNil(t, err, "error expected")
	assert.Nil(t, storer, "storer expected to be nil but got %s", storer)
}

func TestNewStorageUnitFromConf_WrongDBConfig(t *testing.T) {
	t.Parallel()

	storer, err := factory.NewStorageUnitFromConf(common.CacheConfig{
		Capacity: 10,
		Type:     common.LRUCache,
	}, common.DBConfig{
		FilePath: "Blocks",
		Type:     "NotLvlDB",
	},
	)

	assert.NotNil(t, err, "error expected")
	assert.Nil(t, storer, "storer expected to be nil but got %s", storer)
}

func TestNewStorageUnitFromConf_LvlDBOk(t *testing.T) {
	t.Parallel()

	storer, err := factory.NewStorageUnitFromConf(common.CacheConfig{
		Capacity: 10,
		Type:     common.LRUCache,
	}, common.DBConfig{
		FilePath:          "Blocks",
		Type:              common.LvlDB,
		MaxBatchSize:      1,
		BatchDelaySeconds: 1,
		MaxOpenFiles:      10,
	},
	)

	assert.Nil(t, err, "no error expected but got %s", err)
	assert.NotNil(t, storer, "valid storer expected but got nil")
	err = storer.DestroyUnit()
	assert.Nil(t, err, "no error expected destroying the persister")
}

func TestNewStorageUnitFromConf_ShouldWorkLvlDB(t *testing.T) {
	t.Parallel()

	storer, err := factory.NewStorageUnitFromConf(common.CacheConfig{
		Capacity: 10,
		Type:     common.LRUCache,
	}, common.DBConfig{
		FilePath:          "Blocks",
		Type:              common.LvlDB,
		BatchDelaySeconds: 1,
		MaxBatchSize:      1,
		MaxOpenFiles:      10,
	},
	)

	assert.Nil(t, err, "no error expected but got %s", err)
	assert.NotNil(t, storer, "valid storer expected but got nil")
	err = storer.DestroyUnit()
	assert.Nil(t, err, "no error expected destroying the persister")
}
