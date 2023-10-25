package rtcache

import (
	"testing"

	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/testscommon"
	"github.com/multiversx/mx-chain-storage-go/types"
	"github.com/stretchr/testify/assert"
)

func TestNewRemovalTrackingCache(t *testing.T) {
	t.Parallel()

	mainCache := testscommon.NewCacherMock()
	removalCache := testscommon.NewCacherMock()

	t.Run("nil main cache should error", func(t *testing.T) {
		t.Parallel()

		rtCache, err := NewRemovalTrackingCache(nil, removalCache)
		assert.ErrorIs(t, err, common.ErrNilCacher)
		assert.Contains(t, err.Error(), "main cache")
		assert.Nil(t, rtCache)
	})
	t.Run("nil removal cache should error", func(t *testing.T) {
		t.Parallel()

		rtCache, err := NewRemovalTrackingCache(mainCache, nil)
		assert.ErrorIs(t, err, common.ErrNilCacher)
		assert.Contains(t, err.Error(), "removal cache")
		assert.Nil(t, rtCache)
	})
	t.Run("should work", func(t *testing.T) {
		t.Parallel()

		rtCache, err := NewRemovalTrackingCache(mainCache, removalCache)
		assert.Nil(t, err)
		assert.NotNil(t, rtCache)
	})
}

func TestRemovalTrackingCache_IsInterfaceNil(t *testing.T) {
	t.Parallel()

	var rtCache *removalTrackingCache
	assert.True(t, rtCache.IsInterfaceNil())

	rtCache, _ = NewRemovalTrackingCache(testscommon.NewCacherMock(), testscommon.NewCacherMock())
	assert.False(t, rtCache.IsInterfaceNil())
}

func TestRemovalTrackingCache_GetRemovalStatus(t *testing.T) {
	t.Parallel()

	mainCache := testscommon.NewCacherMock()
	removalCache := testscommon.NewCacherMock()
	rtCache, _ := NewRemovalTrackingCache(mainCache, removalCache)

	key := []byte("key")
	value := []byte("value")

	// the node does not know about the key
	assert.Equal(t, types.UnknownRemovalStatus, rtCache.GetRemovalStatus(key))

	// we add the key
	rtCache.Put(key, value, 0)
	assert.True(t, rtCache.Has(key))
	assert.Equal(t, types.NotRemovedStatus, rtCache.GetRemovalStatus(key))

	// we remove the key
	rtCache.Remove(key)
	assert.False(t, rtCache.Has(key))
	assert.Equal(t, types.ExplicitlyRemovedStatus, rtCache.GetRemovalStatus(key))

	// due to evictions or manual clear calls we should not know about the key
	rtCache.Clear()
	assert.False(t, rtCache.Has(key))
	// now the removal tracking cache does not know about the key
	assert.Equal(t, types.UnknownRemovalStatus, rtCache.GetRemovalStatus(key))
}
