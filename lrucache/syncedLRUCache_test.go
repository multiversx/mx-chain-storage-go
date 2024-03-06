package lrucache_test

import (
	"sync"
	"testing"
	"time"

	"github.com/multiversx/mx-chain-storage-go/lrucache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSyncedLRUCache(t *testing.T) {
	t.Parallel()

	t.Run("invalid size should error", func(t *testing.T) {
		c, err := lrucache.NewSyncedLRUCache(-1)
		require.Nil(t, c)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "must provide a positive size")
	})
	t.Run("should work", func(t *testing.T) {
		c, err := lrucache.NewSyncedLRUCache(1)
		require.NotNil(t, c)
		require.Nil(t, err)
	})
}

func TestSyncedLRUCache_IsInterfaceNil(t *testing.T) {
	t.Parallel()

	c, _ := lrucache.NewSyncedLRUCache(-1)
	assert.True(t, c.IsInterfaceNil())

	c, _ = lrucache.NewSyncedLRUCache(100)
	assert.False(t, c.IsInterfaceNil())
}

func TestSyncedLruCache_PutShouldCallTheHandlersInASyncedManner(t *testing.T) {
	t.Parallel()

	c, err := lrucache.NewSyncedLRUCache(100)
	require.Nil(t, err)

	wgCalled := sync.WaitGroup{}
	wgCalled.Add(2)
	handler1 := func(key []byte, value interface{}) {
		wgCalled.Done()
		time.Sleep(time.Second * 2)
	}
	handler2 := func(key []byte, value interface{}) {
		wgCalled.Done()
		time.Sleep(time.Second * 2)
	}

	c.RegisterHandler(handler1, "id1")
	c.RegisterHandler(handler2, "id2")

	timeStampStart := time.Now()
	_ = c.Put([]byte("key"), []byte("value"), 0)
	wgCalled.Wait()
	timeStampEnd := time.Now()

	assert.GreaterOrEqual(t, timeStampEnd.Sub(timeStampStart), time.Second*4)
}
