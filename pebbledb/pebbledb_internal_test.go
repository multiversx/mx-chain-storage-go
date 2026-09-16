package pebbledb

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multiversx/mx-chain-storage-go/common"
)

func TestDB_ReadsSeeTheBatchBeingCommitted(t *testing.T) {
	t.Parallel()

	db, err := NewDB(ArgsNewDB{Path: t.TempDir(), BatchDelaySeconds: 100, MaxBatchSize: 100, MaxOpenFiles: 10})
	require.Nil(t, err)
	defer func() {
		_ = db.Close()
	}()

	key, val := []byte("key"), []byte("value")
	removedKey := []byte("removed")
	flushing := newPendingBatch()
	flushing.put(key, val)
	flushing.remove(removedKey)
	db.setFlushing(flushing)

	res, err := db.Get(key)
	require.Nil(t, err)
	require.Equal(t, val, res)
	require.Nil(t, db.Has(key))

	res, err = db.Get(removedKey)
	require.Nil(t, res)
	require.Equal(t, common.ErrKeyNotFound, err)

	// the pending batch wins over the one being committed
	require.Nil(t, db.Put(removedKey, val))
	require.Nil(t, db.Has(removedKey))
	require.Nil(t, db.Remove(key))
	require.Equal(t, common.ErrKeyNotFound, db.Has(key))

	db.setFlushing(nil)
	require.Equal(t, common.ErrKeyNotFound, db.Has(key))
}

func TestPendingBatch_LastOperationWins(t *testing.T) {
	t.Parallel()

	batch := newPendingBatch()
	require.True(t, batch.isEmpty())

	key := []byte("key")
	batch.put(key, []byte("v1"))
	batch.put(key, []byte("v2"))
	val, removed, found := batch.lookup(key)
	require.False(t, removed)
	require.True(t, found)
	require.Equal(t, []byte("v2"), val)

	batch.remove(key)
	_, removed, found = batch.lookup(key)
	require.True(t, removed)
	require.False(t, found)
	require.Len(t, batch.puts, 0)
	require.Len(t, batch.deletes, 1)

	batch.put(key, []byte("v3"))
	val, removed, found = batch.lookup(key)
	require.False(t, removed)
	require.True(t, found)
	require.Equal(t, []byte("v3"), val)
	require.Len(t, batch.deletes, 0)
	require.False(t, batch.isEmpty())
}

func TestAccessGuard(t *testing.T) {
	t.Parallel()

	t.Run("close waits for the in-flight calls and refuses new ones", func(t *testing.T) {
		t.Parallel()

		guard := newAccessGuard()
		require.True(t, guard.acquire())
		require.False(t, guard.isClosed())

		closed := make(chan bool)
		go func() {
			closed <- guard.closeAndWait()
		}()

		select {
		case <-closed:
			require.Fail(t, "close must wait for the in-flight call")
		case <-time.After(100 * time.Millisecond):
		}

		require.False(t, guard.acquire())
		guard.release()
		require.True(t, <-closed)
		require.True(t, guard.isClosed())
	})
	t.Run("only the first close performs the engine close", func(t *testing.T) {
		t.Parallel()

		guard := newAccessGuard()
		results := make([]bool, 10)
		wg := sync.WaitGroup{}
		wg.Add(len(results))
		for i := range results {
			go func(idx int) {
				defer wg.Done()
				results[idx] = guard.closeAndWait()
			}(i)
		}
		wg.Wait()

		numPerformed := 0
		for _, performed := range results {
			if performed {
				numPerformed++
			}
		}
		require.Equal(t, 1, numPerformed)
	})
}
