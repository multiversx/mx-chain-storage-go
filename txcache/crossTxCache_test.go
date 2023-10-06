package txcache

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/testscommon"
	"github.com/stretchr/testify/require"
)

func TestCrossTxCache_DoImmunizeTxsAgainstEviction(t *testing.T) {
	cache := newCrossTxCacheToTest(1, 8, math.MaxUint16)
	defer func() { require.Nil(t, cache.Close()) }()

	cache.addTestTxs("a", "b", "c", "d")
	numNow, numFuture := cache.ImmunizeKeys(hashesAsBytes([]string{"a", "b", "e", "f"}))
	require.Equal(t, 2, numNow)
	require.Equal(t, 2, numFuture)
	require.Equal(t, 4, cache.Len())

	cache.addTestTxs("e", "f", "g", "h")
	require.ElementsMatch(t, []string{"a", "b", "c", "d", "e", "f", "g", "h"}, hashesAsStrings(cache.Keys()))

	cache.addTestTxs("i", "j", "k", "l")
	require.ElementsMatch(t, []string{"a", "b", "e", "f", "i", "j", "k", "l"}, hashesAsStrings(cache.Keys()))
}

func TestCrossTxCache_Get(t *testing.T) {
	cache := newCrossTxCacheToTest(1, 8, math.MaxUint16)
	defer func() { require.Nil(t, cache.Close()) }()

	cache.addTestTxs("a", "b", "c", "d")
	a, ok := cache.GetByTxHash([]byte("a"))
	require.True(t, ok)
	require.NotNil(t, a)

	x, ok := cache.GetByTxHash([]byte("x"))
	require.False(t, ok)
	require.Nil(t, x)

	aTx, ok := cache.Get([]byte("a"))
	require.True(t, ok)
	require.NotNil(t, aTx)
	require.Equal(t, a.Tx, aTx)

	xTx, ok := cache.Get([]byte("x"))
	require.False(t, ok)
	require.Nil(t, xTx)

	aTx, ok = cache.Peek([]byte("a"))
	require.True(t, ok)
	require.NotNil(t, aTx)
	require.Equal(t, a.Tx, aTx)

	xTx, ok = cache.Peek([]byte("x"))
	require.False(t, ok)
	require.Nil(t, xTx)

	require.Equal(t, make([]*WrappedTransaction, 0), cache.GetTransactionsPoolForSender(""))
}

func TestCrossTxCache_RegisterEvictionHandler(t *testing.T) {
	t.Parallel()

	cache := newCrossTxCacheToTest(1, 8, math.MaxUint16)
	defer func() { require.Nil(t, cache.Close()) }()

	cache.addTestTx("hash-1")

	err := cache.RegisterEvictionHandler(nil)
	require.Equal(t, common.ErrNilEvictionHandler, err)

	ch := make(chan struct{})
	err = cache.RegisterEvictionHandler(&testscommon.EvictionNotifierStub{
		NotifyEvictionCalled: func(hash []byte, cacheId string) {
			require.True(t, bytes.Equal([]byte("hash-1"), hash))
			ch <- struct{}{}
		},
		ShouldNotifyEvictionCalled: func(txHash []byte) bool {
			return true
		},
	})
	require.NoError(t, err)

	removed := cache.RemoveTxByHash([]byte("hash-1"))
	require.True(t, removed)
	select {
	case <-ch:
	case <-time.After(time.Second):
		require.Fail(t, "timeout")
	}

	foundTx, ok := cache.GetByTxHash([]byte("hash-1"))
	require.False(t, ok)
	require.Nil(t, foundTx)
}

func newCrossTxCacheToTest(numChunks uint32, maxNumItems uint32, numMaxBytes uint32) *CrossTxCache {
	cache, err := NewCrossTxCache(ConfigDestinationMe{
		Name:                        "test",
		NumChunks:                   numChunks,
		MaxNumItems:                 maxNumItems,
		MaxNumBytes:                 numMaxBytes,
		NumItemsToPreemptivelyEvict: numChunks * 1,
	})
	if err != nil {
		panic(fmt.Sprintf("newCrossTxCacheToTest(): %s", err))
	}

	return cache
}

func (cache *CrossTxCache) addTestTxs(hashes ...string) {
	for _, hash := range hashes {
		_, _ = cache.addTestTx(hash)
	}
}

func (cache *CrossTxCache) addTestTx(hash string) (ok, added bool) {
	return cache.AddTx(createTx([]byte(hash), ".", uint64(42)))
}
