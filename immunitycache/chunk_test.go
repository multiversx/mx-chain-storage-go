package immunitycache

import (
	"math"
	"testing"

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

func TestImmunityChunk_AddItemEvictsImmuneCandidatesInDistanceOrder(t *testing.T) {
	chunk := newChunkToTest(3, math.MaxUint32)
	chunk.addTestItems("x", "y", "z")

	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"x"}), 1)
	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"y"}), 5)
	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"z"}), 8)
	_, _ = chunk.ImmunizeKeys(keysAsBytes([]string{"incoming"}), 6)

	has, added := chunk.AddItem(newCacheItem("foo", "incoming", 1))
	require.False(t, has)
	require.True(t, added)
	require.Equal(t, []string{"y", "z", "incoming"}, keysAsStrings(chunk.KeysInOrder()))
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
	})

	return chunk
}

func newChunkToTest(maxNumItems uint32, numMaxBytes uint32) *immunityChunk {
	chunk := newImmunityChunk(immunityChunkConfig{
		maxNumItems:                 maxNumItems,
		maxNumBytes:                 numMaxBytes,
		numItemsToPreemptivelyEvict: 1,
	})

	return chunk
}

func (chunk *immunityChunk) addTestItems(keys ...string) {
	for _, key := range keys {
		_, _ = chunk.AddItem(newCacheItem("foo", key, 100))
	}
}
