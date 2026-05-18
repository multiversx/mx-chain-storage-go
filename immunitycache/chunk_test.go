package immunitycache

import (
	"math"
	"testing"

	"github.com/multiversx/mx-chain-core-go/core/atomic"
	"github.com/stretchr/testify/require"
)

func TestImmunityChunk_ImmunizeKeys(t *testing.T) {
	chunk := newUnconstrainedChunkToTest()

	chunk.addTestItems("x", "y", "z")
	require.Equal(t, 3, chunk.Count())

	// No immune items, all removed
	numRemoved := chunk.RemoveOldest(42)
	require.Equal(t, 3, numRemoved)
	require.Equal(t, 0, chunk.Count())

	chunk.addTestItems("x", "y", "z")
	require.Equal(t, 3, chunk.Count())

	// Immunize some items
	numNow, numFuture := chunk.ImmunizeKeys(keysAsBytes([]string{"x", "z"}), 7)
	require.Equal(t, 2, numNow)
	require.Equal(t, 0, numFuture)

	numRemoved = chunk.RemoveOldest(42)
	require.Equal(t, 1, numRemoved)
	require.Equal(t, 2, chunk.Count())
	require.Equal(t, []string{"x", "z"}, keysAsStrings(chunk.KeysInOrder()))
}

func TestImmunityChunk_AddItemIgnoresDuplicates(t *testing.T) {
	chunk := newUnconstrainedChunkToTest()
	chunk.addTestItems("x", "y", "z")
	require.Equal(t, 3, chunk.Count())

	has, added := chunk.AddItem(newCacheItem("foo", "a", 1))
	require.False(t, has)
	require.True(t, added)
	require.Equal(t, 4, chunk.Count())

	has, added = chunk.AddItem(newCacheItem("bar", "x", 1))
	require.True(t, has)
	require.False(t, added)
	require.Equal(t, 4, chunk.Count())
}

func TestImmunityChunk_AddItemEvictsWhenTooMany(t *testing.T) {
	chunk := newChunkToTest(3, math.MaxUint32)
	chunk.addTestItems("x", "y", "z")
	require.Equal(t, 3, chunk.Count())

	chunk.addTestItems("a", "b")
	require.Equal(t, []string{"z", "a", "b"}, keysAsStrings(chunk.KeysInOrder()))
}

func TestImmunityChunk_AddItemDoesNotEvictImmuneItems(t *testing.T) {
	chunk := newChunkToTest(3, math.MaxUint32)
	chunk.addTestItems("x", "y", "z")
	require.Equal(t, 3, chunk.Count())

	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"x", "y"}), 7)

	chunk.addTestItems("a")
	require.Equal(t, []string{"x", "y", "a"}, keysAsStrings(chunk.KeysInOrder()))
	chunk.addTestItems("b")
	require.Equal(t, []string{"x", "y", "b"}, keysAsStrings(chunk.KeysInOrder()))

	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"b"}), 7)
	has, added := chunk.AddItem(newCacheItem("foo", "c", 1))
	require.False(t, has)
	require.False(t, added)
	require.Equal(t, []string{"x", "y", "b"}, keysAsStrings(chunk.KeysInOrder()))
}

func TestImmunityChunk_ImmunizeKeysSmartRejectDisplacesHighestNonce(t *testing.T) {
	chunk := newChunkToTest(3, math.MaxUint32)
	chunk.addTestItems("x", "y", "z")

	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"x"}), 1)
	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"y"}), 5)
	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"z"}), 8)
	require.Equal(t, uint64(8), chunk.maxImmuneNonce)

	// "incoming" at nonce 6 is closer to the de-immunization frontier than z@8.
	// z must be displaced (and evicted, since it's in-cache).
	numNow, numFuture := chunk.ImmunizeKeys(keysAsBytes([]string{"incoming"}), 6)
	require.Equal(t, 0, numNow)
	require.Equal(t, 1, numFuture)

	require.Equal(t, []string{"x", "y"}, keysAsStrings(chunk.KeysInOrder()))
	require.Equal(t, uint64(6), chunk.maxImmuneNonce)
	require.Equal(t, 3, chunk.CountImmune())
}

func TestImmunityChunk_ImmunizeKeysSmartRejectSkipsWhenIncomingNotCloser(t *testing.T) {
	chunk := newChunkToTest(3, math.MaxUint32)
	chunk.addTestItems("x", "y", "z")
	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"x", "y", "z"}), 5)
	require.Equal(t, uint64(5), chunk.maxImmuneNonce)
	require.Equal(t, 3, chunk.CountImmune())

	// Incoming at nonce 10 is FURTHER than the existing max, so it is silently skipped.
	numNow, numFuture := chunk.ImmunizeKeys(keysAsBytes([]string{"incoming"}), 10)
	require.Equal(t, 0, numNow)
	require.Equal(t, 0, numFuture)
	require.Equal(t, 3, chunk.CountImmune())
	require.NotContains(t, chunk.immuneKeys, "incoming")
	require.Equal(t, uint64(5), chunk.maxImmuneNonce)
}

func TestImmunityChunk_ImmunizeKeysSkipsBelowOldestImmuneNonce(t *testing.T) {
	chunk := newUnconstrainedChunkToTest()
	chunk.SetOldestImmuneNonce(10)

	numNow, numFuture := chunk.ImmunizeKeys(keysAsBytes([]string{"a", "b"}), 5)
	require.Equal(t, 0, numNow)
	require.Equal(t, 0, numFuture)
	require.Equal(t, 0, chunk.CountImmune())
}

func TestImmunityChunk_ImmunizeKeysUpgradesExistingNonce(t *testing.T) {
	chunk := newUnconstrainedChunkToTest()
	chunk.addTestItems("a")

	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"a"}), 5)
	require.Equal(t, uint64(5), chunk.immuneKeys["a"])
	require.Equal(t, uint64(5), chunk.maxImmuneNonce)

	// Upgrade to higher nonce.
	numNow, _ := chunk.ImmunizeKeys(keysAsBytes([]string{"a"}), 9)
	require.Equal(t, 1, numNow)
	require.Equal(t, uint64(9), chunk.immuneKeys["a"])
	require.Equal(t, uint64(9), chunk.maxImmuneNonce)
	require.Len(t, chunk.nonceToKeys, 1)

	// Downgrade is silently skipped (item & map keep the higher nonce).
	numNow, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"a"}), 3)
	require.Equal(t, 0, numNow)
	require.Equal(t, uint64(9), chunk.immuneKeys["a"])
}

func TestImmunityChunk_AddItemEvictsHighestImmuneNonceUnderPressure(t *testing.T) {
	// All slots immune; incoming is immune at a lower nonce, so it displaces the highest in-cache.
	chunk := newChunkToTest(3, math.MaxUint32)
	chunk.addTestItems("a", "b", "c")

	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"a"}), 2)
	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"b"}), 5)
	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"c"}), 8)
	// Pre-register an intent for "d" at nonce 3; smart-reject displaces c@8.
	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"d"}), 3)
	require.NotContains(t, chunk.immuneKeys, "c")
	require.Equal(t, uint64(5), chunk.maxImmuneNonce)

	has, added := chunk.AddItem(newCacheItem("foo", "d", 1))
	require.False(t, has)
	require.True(t, added)
	require.ElementsMatch(t, []string{"a", "b", "d"}, keysAsStrings(chunk.KeysInOrder()))
}

func TestImmunityChunk_MaxImmuneNonceRecomputesWhenLastAtMaxIsRemoved(t *testing.T) {
	chunk := newUnconstrainedChunkToTest()
	chunk.addTestItems("a", "b")
	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"a"}), 5)
	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"b"}), 9)
	require.Equal(t, uint64(9), chunk.maxImmuneNonce)

	chunk.RemoveItem("b")
	require.Equal(t, uint64(5), chunk.maxImmuneNonce)

	chunk.RemoveItem("a")
	require.Equal(t, uint64(0), chunk.maxImmuneNonce)
}

func TestImmunityChunk_NonceToKeysInvariantAfterMixedOps(t *testing.T) {
	chunk := newUnconstrainedChunkToTest()
	chunk.addTestItems("a", "b", "c", "d")
	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"a", "b"}), 5)
	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"c", "d"}), 9)
	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"future-x"}), 9)

	requireChunkInvariants(t, chunk)

	chunk.RemoveItem("a")
	requireChunkInvariants(t, chunk)

	chunk.SetOldestImmuneNonce(6) // drops a,b intents (and a was already gone)
	requireChunkInvariants(t, chunk)
	require.Equal(t, uint64(9), chunk.maxImmuneNonce)
	require.NotContains(t, chunk.immuneKeys, "b")

	chunk.SetOldestImmuneNonce(10) // drops everything
	requireChunkInvariants(t, chunk)
	require.Equal(t, uint64(0), chunk.maxImmuneNonce)
	require.Equal(t, 0, chunk.CountImmune())
}

func requireChunkInvariants(t *testing.T, chunk *immunityChunk) {
	t.Helper()
	// (a) immuneKeys and nonceToKeys agree on counts.
	total := 0
	for _, bucket := range chunk.nonceToKeys {
		total += len(bucket)
	}
	require.Equal(t, len(chunk.immuneKeys), total, "immuneKeys/nonceToKeys counts diverged")

	// (b) every immuneKeys entry lives in its declared bucket.
	for k, n := range chunk.immuneKeys {
		bucket, ok := chunk.nonceToKeys[n]
		require.True(t, ok, "missing bucket for key %s @ nonce %d", k, n)
		_, inBucket := bucket[k]
		require.True(t, inBucket, "key %s not in bucket %d", k, n)
	}

	// (c) maxImmuneNonce matches the actual maximum (0 when empty).
	var expectedMax uint64
	for n := range chunk.nonceToKeys {
		if n > expectedMax {
			expectedMax = n
		}
	}
	require.Equal(t, expectedMax, chunk.maxImmuneNonce, "maxImmuneNonce stale")
}

func TestImmunityChunk_SetOldestImmuneNonceDeactivatesImmuneItemsBelowThreshold(t *testing.T) {
	chunk := newUnconstrainedChunkToTest()
	chunk.addTestItems("x", "y", "z")

	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"x", "y"}), 7)
	require.Equal(t, 2, chunk.CountImmune())

	chunk.SetOldestImmuneNonce(8)
	require.Equal(t, 0, chunk.CountImmune())

	numRemoved := chunk.RemoveOldest(42)
	require.Equal(t, 3, numRemoved)
	require.Equal(t, 0, chunk.Count())
}

func TestImmunityChunk_SetOldestImmuneNonceCleansInactiveFutureImmuneKeys(t *testing.T) {
	chunk := newUnconstrainedChunkToTest()

	_, numFuture := chunk.ImmunizeKeys(keysAsBytes([]string{"future-a", "future-b"}), 7)
	require.Equal(t, 2, numFuture)
	require.Len(t, chunk.immuneKeys, 2)
	require.Equal(t, 2, chunk.CountImmune())

	chunk.SetOldestImmuneNonce(8)
	require.Len(t, chunk.immuneKeys, 0)
	require.Equal(t, 0, chunk.CountImmune())
}

func TestImmunityChunk_AddItemEvictsPreviouslyImmuneItemsAfterThresholdAdvance(t *testing.T) {
	chunk := newChunkToTest(3, math.MaxUint32)
	chunk.addTestItems("x", "y", "z")

	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"x", "y", "z"}), 7)
	require.Equal(t, 3, chunk.CountImmune())

	chunk.SetOldestImmuneNonce(8)
	require.Equal(t, 0, chunk.CountImmune())

	has, added := chunk.AddItem(newCacheItem("foo", "incoming", 1))
	require.False(t, has)
	require.True(t, added)
	require.Equal(t, []string{"y", "z", "incoming"}, keysAsStrings(chunk.KeysInOrder()))
}

func newUnconstrainedChunkToTest() *immunityChunk {
	chunk := newImmunityChunk(immunityChunkConfig{
		maxNumItems:                 math.MaxUint32,
		maxNumBytes:                 maxNumBytesUpperBound,
		numItemsToPreemptivelyEvict: math.MaxUint32,
	}, &atomic.Counter{})

	return chunk
}

func newChunkToTest(maxNumItems uint32, numMaxBytes uint32) *immunityChunk {
	chunk := newImmunityChunk(immunityChunkConfig{
		maxNumItems:                 maxNumItems,
		maxNumBytes:                 numMaxBytes,
		numItemsToPreemptivelyEvict: 1,
	}, &atomic.Counter{})

	return chunk
}

func (chunk *immunityChunk) addTestItems(keys ...string) {
	for _, key := range keys {
		_, _ = chunk.AddItem(newCacheItem("foo", key, 100))
	}
}
