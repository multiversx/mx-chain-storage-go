package factory_test

import (
	"testing"

	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/factory"
	"github.com/stretchr/testify/assert"
)

func TestCreateCacheFromConfWrongType(t *testing.T) {

	cacher, err := factory.NewCache(common.CacheConfig{Type: "NotLRU", Capacity: 100, Shards: 1, SizeInBytes: 0})

	assert.NotNil(t, err, "error expected")
	assert.Nil(t, cacher, "cacher expected to be nil, but got %s", cacher)
}

func TestCreateCacheFromConfOK(t *testing.T) {

	cacher, err := factory.NewCache(common.CacheConfig{Type: common.LRUCache, Capacity: 10, Shards: 1, SizeInBytes: 0})

	assert.Nil(t, err, "no error expected but got %s", err)
	assert.NotNil(t, cacher, "valid cacher expected but got nil")
}
