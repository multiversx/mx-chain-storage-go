package lrucache_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/multiversx/mx-chain-core-go/core/check"
	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/lrucache"
	"github.com/multiversx/mx-chain-storage-go/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var timeoutWaitForWaitGroups = time.Second * 2

//------- NewCache

func TestNewCache_BadSizeShouldErr(t *testing.T) {
	t.Parallel()

	c, err := lrucache.NewCache(0, false)

	assert.True(t, check.IfNil(c))
	assert.NotNil(t, err)
}

func TestNewCache_ShouldWork(t *testing.T) {
	t.Parallel()

	c, err := lrucache.NewCache(1, false)

	assert.False(t, check.IfNil(c))
	assert.Nil(t, err)
}

//------- NewCacheWithSizeInBytes

func TestNewCacheWithSizeInBytes_BadSizeShouldErr(t *testing.T) {
	t.Parallel()

	c, err := lrucache.NewCacheWithSizeInBytes(0, 100000, false)

	assert.True(t, check.IfNil(c))
	assert.Equal(t, common.ErrCacheSizeInvalid, err)
}

func TestNewCacheWithSizeInBytes_BadSizeInBytesShouldErr(t *testing.T) {
	t.Parallel()

	c, err := lrucache.NewCacheWithSizeInBytes(1, 0, false)

	assert.True(t, check.IfNil(c))
	assert.Equal(t, common.ErrCacheCapacityInvalid, err)
}

func TestNewCacheWithSizeInBytes_ShouldWork(t *testing.T) {
	t.Parallel()

	c, err := lrucache.NewCacheWithSizeInBytes(1, 100000, false)

	assert.False(t, check.IfNil(c))
	assert.Nil(t, err)
}

func TestLRUCache_PutNotPresent(t *testing.T) {
	t.Parallel()

	key, val := []byte("key"), []byte("value")
	c, _ := lrucache.NewCache(10, false)

	l := c.Len()

	assert.Zero(t, l, "cache expected to be empty")

	c.Put(key, val, 0)
	l = c.Len()

	assert.Equal(t, l, 1, "cache size expected 1 but found %d", l)
}

func TestLRUCache_PutPresent(t *testing.T) {
	t.Parallel()

	key, val := []byte("key"), []byte("value")
	c, _ := lrucache.NewCache(10, false)

	c.Put(key, val, 0)
	c.Put(key, val, 0)

	l := c.Len()
	assert.Equal(t, l, 1, "cache size expected 1 but found %d", l)
}

func TestLRUCache_PutPresentRewrite(t *testing.T) {
	t.Parallel()

	key := []byte("key")
	val1 := []byte("value1")
	val2 := []byte("value2")
	c, _ := lrucache.NewCache(10, false)

	c.Put(key, val1, 0)
	c.Put(key, val2, 0)

	l := c.Len()
	assert.Equal(t, l, 1, "cache size expected 1 but found %d", l)
	recoveredVal, has := c.Get(key)
	assert.True(t, has)
	assert.Equal(t, val2, recoveredVal)
}

func TestLRUCache_GetNotPresent(t *testing.T) {
	t.Parallel()

	key := []byte("key1")
	c, _ := lrucache.NewCache(10, false)

	v, ok := c.Get(key)

	assert.False(t, ok, "value %s not expected to be found", v)
}

func TestLRUCache_GetPresent(t *testing.T) {
	t.Parallel()

	key, val := []byte("key2"), []byte("value2")
	c, _ := lrucache.NewCache(10, false)

	c.Put(key, val, 0)

	v, ok := c.Get(key)

	assert.True(t, ok, "value expected but not found")
	assert.Equal(t, val, v)
}

func TestLRUCache_HasNotPresent(t *testing.T) {
	t.Parallel()

	key := []byte("key3")
	c, _ := lrucache.NewCache(10, false)

	found := c.Has(key)

	assert.False(t, found, "key %s not expected to be found", key)
}

func TestLRUCache_HasPresent(t *testing.T) {
	t.Parallel()

	key, val := []byte("key4"), []byte("value4")
	c, _ := lrucache.NewCache(10, false)

	c.Put(key, val, 0)

	found := c.Has(key)

	assert.True(t, found, "value expected but not found")
}

func TestLRUCache_PeekNotPresent(t *testing.T) {
	t.Parallel()

	key := []byte("key5")
	c, _ := lrucache.NewCache(10, false)

	_, ok := c.Peek(key)

	assert.False(t, ok, "not expected to find key %s", key)
}

func TestLRUCache_PeekPresent(t *testing.T) {
	t.Parallel()

	key, val := []byte("key6"), []byte("value6")
	c, _ := lrucache.NewCache(10, false)

	c.Put(key, val, 0)
	v, ok := c.Peek(key)

	assert.True(t, ok, "value expected but not found")
	assert.Equal(t, val, v, "expected to find %s but found %s", val, v)
}

func TestLRUCache_HasOrAddNotPresent(t *testing.T) {
	t.Parallel()

	key, val := []byte("key7"), []byte("value7")
	c, _ := lrucache.NewCache(10, false)

	_, ok := c.Peek(key)
	assert.False(t, ok, "not expected to find key %s", key)

	c.HasOrAdd(key, val, 0)
	v, ok := c.Peek(key)
	assert.True(t, ok, "value expected but not found")
	assert.Equal(t, val, v, "expected to find %s but found %s", val, v)
}

func TestLRUCache_HasOrAddPresent(t *testing.T) {
	t.Parallel()

	key, val := []byte("key8"), []byte("value8")
	c, _ := lrucache.NewCache(10, false)

	_, ok := c.Peek(key)

	assert.False(t, ok, "not expected to find key %s", key)

	c.HasOrAdd(key, val, 0)
	v, ok := c.Peek(key)

	assert.True(t, ok, "value expected but not found")
	assert.Equal(t, val, v, "expected to find %s but found %s", val, v)
}

func TestLRUCache_RemoveNotPresent(t *testing.T) {
	t.Parallel()

	key := []byte("key9")
	c, _ := lrucache.NewCache(10, false)

	found := c.Has(key)

	assert.False(t, found, "not expected to find key %s", key)

	c.Remove(key)
	found = c.Has(key)

	assert.False(t, found, "not expected to find key %s", key)
}

func TestLRUCache_RemovePresent(t *testing.T) {
	t.Parallel()

	key, val := []byte("key10"), []byte("value10")
	c, _ := lrucache.NewCache(10, false)

	c.Put(key, val, 0)
	found := c.Has(key)

	assert.True(t, found, "expected to find key %s", key)

	c.Remove(key)
	found = c.Has(key)

	assert.False(t, found, "not expected to find key %s", key)
}

func TestLRUCache_Keys(t *testing.T) {
	t.Parallel()

	c, _ := lrucache.NewCache(10, false)

	for i := 0; i < 20; i++ {
		key, val := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i))
		c.Put(key, val, 0)
	}

	keys := c.Keys()

	// check also that cache size does not grow over the capacity
	assert.Equal(t, 10, len(keys), "expected cache size 10 but current size %d", len(keys))
}

func TestLRUCache_Len(t *testing.T) {
	t.Parallel()

	c, _ := lrucache.NewCache(10, false)

	for i := 0; i < 20; i++ {
		key, val := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i))
		c.Put(key, val, 0)
	}

	l := c.Len()

	assert.Equal(t, 10, l, "expected cache size 10 but current size %d", l)
}

func TestLRUCache_Clear(t *testing.T) {
	t.Parallel()

	c, _ := lrucache.NewCache(10, false)

	for i := 0; i < 5; i++ {
		key, val := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i))
		c.Put(key, val, 0)
	}

	l := c.Len()

	assert.Equal(t, 5, l, "expected size 5, got %d", l)

	c.Clear()
	l = c.Len()

	assert.Zero(t, l, "expected size 0, got %d", l)
}

func TestLRUCache_CacherRegisterAddedDataHandlerNilHandlerShouldIgnore(t *testing.T) {
	t.Parallel()

	c, _ := lrucache.NewCache(100, false)
	c.RegisterHandler(nil, "")

	assert.Equal(t, 0, len(c.AddedDataHandlers()))
}

func TestLRUCache_CacherRegisterPutAddedDataHandlerShouldWork(t *testing.T) {
	t.Parallel()

	wg := sync.WaitGroup{}
	wg.Add(1)
	chDone := make(chan bool)

	f := func(key []byte, value interface{}) {
		if !bytes.Equal([]byte("aaaa"), key) {
			return
		}

		wg.Done()
	}

	go func() {
		wg.Wait()
		chDone <- true
	}()

	c, _ := lrucache.NewCache(100, false)
	c.RegisterHandler(f, "")
	c.Put([]byte("aaaa"), "bbbb", 0)

	select {
	case <-chDone:
	case <-time.After(timeoutWaitForWaitGroups):
		assert.Fail(t, "should have been called")
		return
	}

	assert.Equal(t, 1, len(c.AddedDataHandlers()))
}

func TestLRUCache_CacherRegisterHasOrAddAddedDataHandlerShouldWork(t *testing.T) {
	t.Parallel()

	wg := sync.WaitGroup{}
	wg.Add(1)
	chDone := make(chan bool)

	f := func(key []byte, value interface{}) {
		if !bytes.Equal([]byte("aaaa"), key) {
			return
		}

		wg.Done()
	}

	go func() {
		wg.Wait()
		chDone <- true
	}()

	c, _ := lrucache.NewCache(100, false)
	c.RegisterHandler(f, "")
	c.HasOrAdd([]byte("aaaa"), "bbbb", 0)

	select {
	case <-chDone:
	case <-time.After(timeoutWaitForWaitGroups):
		assert.Fail(t, "should have been called")
		return
	}

	assert.Equal(t, 1, len(c.AddedDataHandlers()))
}

func TestLRUCache_CacherRegisterHasOrAddAddedDataHandlerNotAddedShouldNotCall(t *testing.T) {
	t.Parallel()

	wg := sync.WaitGroup{}
	wg.Add(1)
	chDone := make(chan bool)

	f := func(key []byte, value interface{}) {
		wg.Done()
	}

	go func() {
		wg.Wait()
		chDone <- true
	}()

	c, _ := lrucache.NewCache(100, false)
	//first add, no call
	c.HasOrAdd([]byte("aaaa"), "bbbb", 0)
	c.RegisterHandler(f, "")
	//second add, should not call as the data was found
	c.HasOrAdd([]byte("aaaa"), "bbbb", 0)

	select {
	case <-chDone:
		assert.Fail(t, "should have not been called")
		return
	case <-time.After(timeoutWaitForWaitGroups):
	}

	assert.Equal(t, 1, len(c.AddedDataHandlers()))
}

func TestLRUCache_CloseShouldNotErr(t *testing.T) {
	t.Parallel()

	c, _ := lrucache.NewCache(1, false)

	err := c.Close()
	assert.Nil(t, err)
}

type cacheWrapper struct {
	c types.Cacher
}

func newCacheWrapper() *cacheWrapper {
	wrapper := &cacheWrapper{}
	wrapper.c, _ = lrucache.NewCacheWithEviction(2, wrapper.onEvict, false)

	return wrapper
}

func (wrapper *cacheWrapper) onEvict(_ interface{}, _ interface{}) {
	_ = wrapper.c.Len()
}

func TestLruCache_LenDuringEviction(t *testing.T) {
	t.Parallel()

	key1 := []byte("key 1")
	key2 := []byte("key 2")
	key0 := []byte("key 0")

	chTestDone := make(chan struct{})

	go func() {
		wrapper := newCacheWrapper()
		wrapper.c.Put(key0, struct{}{}, 0)
		wrapper.c.Put(key1, struct{}{}, 0)
		wrapper.c.Put(key2, struct{}{}, 0)

		close(chTestDone)
	}()

	select {
	case <-chTestDone:
	case <-time.After(time.Second):
		assert.Fail(t, "test failed, deadlock occurred")
	}
}

func TestLruCache_PutShouldCallTheHandlers(t *testing.T) {
	t.Parallel()

	t.Run("async mode should work", func(t *testing.T) {
		c, err := lrucache.NewCache(100, false)
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

		assert.Less(t, timeStampEnd.Sub(timeStampStart), time.Second*4)
	})
	t.Run("sync mode should work", func(t *testing.T) {
		c, err := lrucache.NewCache(100, true)
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
	})

}
