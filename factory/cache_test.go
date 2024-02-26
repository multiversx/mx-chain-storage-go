package factory_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/factory"
	"github.com/stretchr/testify/require"
)

func TestNewCache(t *testing.T) {
	t.Parallel()

	t.Run("should fail if wrong cache type", func(t *testing.T) {
		cacheConf := common.CacheConfig{
			Type:        "NotLRU",
			Capacity:    100,
			Shards:      1,
			SizeInBytes: 0,
		}
		cacher, err := factory.NewCache(cacheConf)
		require.Equal(t, common.ErrNotSupportedCacheType, err)
		require.Nil(t, cacher, "cacher expected to be nil, but got %s", cacher)
	})

	t.Run("LRUCache type, with provided size, should fail", func(t *testing.T) {
		t.Parallel()

		cacheConf := common.CacheConfig{
			Type:        common.LRUCache,
			Capacity:    100,
			Shards:      1,
			SizeInBytes: 1024,
		}
		cacher, err := factory.NewCache(cacheConf)
		require.Equal(t, common.ErrLRUCacheWithProvidedSize, err)
		require.Nil(t, cacher)
	})

	t.Run("LRUCache type, should work", func(t *testing.T) {
		t.Parallel()

		cacheConf := common.CacheConfig{
			Type:        common.LRUCache,
			Capacity:    100,
			Shards:      1,
			SizeInBytes: 0,
		}
		cacher, err := factory.NewCache(cacheConf)
		require.Nil(t, err)
		require.Equal(t, "*lrucache.lruCache", fmt.Sprintf("%T", cacher))
	})

	t.Run("SizeLRUCache type, invalid size, should fail", func(t *testing.T) {
		t.Parallel()

		cacheConf := common.CacheConfig{
			Type:        common.SizeLRUCache,
			Capacity:    100,
			Shards:      1,
			SizeInBytes: 512,
		}
		cacher, err := factory.NewCache(cacheConf)
		require.True(t, errors.Is(err, common.ErrLRUCacheInvalidSize))
		require.Nil(t, cacher)
	})

	t.Run("SizeLRUCache type, should work", func(t *testing.T) {
		t.Parallel()

		cacheConf := common.CacheConfig{
			Type:        common.SizeLRUCache,
			Capacity:    100,
			Shards:      1,
			SizeInBytes: 1024,
		}
		cacher, err := factory.NewCache(cacheConf)
		require.Nil(t, err)
		require.Equal(t, "*lrucache.lruCache", fmt.Sprintf("%T", cacher))
	})

	t.Run("FIFOShardedCache type, should work", func(t *testing.T) {
		t.Parallel()

		cacheConf := common.CacheConfig{
			Type:        common.FIFOShardedCache,
			Capacity:    100,
			Shards:      1,
			SizeInBytes: 1024,
		}
		cacher, err := factory.NewCache(cacheConf)
		require.Nil(t, err)
		require.Equal(t, "*fifocache.FIFOShardedCache", fmt.Sprintf("%T", cacher))
	})
}
