package pebbledb_test

import (
	"fmt"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/pebbledb"
)

func createArgs(path string, batchDelaySeconds int, maxBatchSize int) pebbledb.ArgsNewDB {
	return pebbledb.ArgsNewDB{
		Path:                  path,
		BatchDelaySeconds:     batchDelaySeconds,
		MaxBatchSize:          maxBatchSize,
		MaxOpenFiles:          10,
		BloomFilterBitsPerKey: 10,
	}
}

func createPebbleDB(tb testing.TB, batchDelaySeconds int, maxBatchSize int) *pebbledb.DB {
	db, err := pebbledb.NewDB(createArgs(tb.TempDir(), batchDelaySeconds, maxBatchSize))
	require.Nil(tb, err)

	return db
}

func TestNewDB(t *testing.T) {
	t.Parallel()

	t.Run("invalid max open files, should fail", func(t *testing.T) {
		t.Parallel()

		args := createArgs(t.TempDir(), 10, 1)
		args.MaxOpenFiles = 0

		db, err := pebbledb.NewDB(args)
		require.Nil(t, db)
		require.Equal(t, common.ErrInvalidNumOpenFiles, err)
	})
	t.Run("unknown profile, should fail", func(t *testing.T) {
		t.Parallel()

		args := createArgs(t.TempDir(), 10, 1)
		args.Profile = "unknown"

		db, err := pebbledb.NewDB(args)
		require.Nil(t, db)
		require.ErrorIs(t, err, common.ErrInvalidConfig)
	})
	t.Run("all profiles should work", func(t *testing.T) {
		t.Parallel()

		for _, profile := range []string{"", pebbledb.DefaultProfile, pebbledb.HeavyWriteProfile} {
			args := createArgs(t.TempDir(), 10, 1)
			args.Profile = profile

			db, err := pebbledb.NewDB(args)
			require.Nil(t, err, profile)
			require.Nil(t, db.Close())
		}
	})
	t.Run("non positive batch settings, should fail", func(t *testing.T) {
		t.Parallel()

		args := createArgs(t.TempDir(), 0, 1)
		db, err := pebbledb.NewDB(args)
		require.Nil(t, db)
		require.ErrorIs(t, err, common.ErrInvalidConfig)

		args = createArgs(t.TempDir(), 1, 0)
		db, err = pebbledb.NewDB(args)
		require.Nil(t, db)
		require.ErrorIs(t, err, common.ErrInvalidConfig)
	})
	t.Run("path is a file, should fail", func(t *testing.T) {
		t.Parallel()

		filePath := t.TempDir() + "/file"
		require.Nil(t, os.WriteFile(filePath, []byte("data"), 0600))

		db, err := pebbledb.NewDB(createArgs(filePath, 10, 1))
		require.Nil(t, db)
		require.NotNil(t, err)
	})
}

func TestDB_WithoutBloomAndLateWithBloomFilter(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	args := createArgs(tmp, 10, 1)
	args.BloomFilterBitsPerKey = 0

	db, err := pebbledb.NewDB(args)
	require.Nil(t, err)

	key, val := []byte("key"), []byte("value")
	require.Nil(t, db.Put(key, val))
	require.Nil(t, db.Close())

	res, err := db.Get(key)
	require.Nil(t, res)
	require.Equal(t, common.ErrDBIsClosed, err)

	args.BloomFilterBitsPerKey = 10
	db, err = pebbledb.NewDB(args)
	require.Nil(t, err)
	defer func() {
		_ = db.Close()
	}()

	require.Nil(t, db.Put([]byte("new-key"), val))

	res, err = db.Get(key)
	require.Nil(t, err)
	require.Equal(t, val, res)
}

func TestDB_PutNoError(t *testing.T) {
	t.Parallel()

	db := createPebbleDB(t, 10, 1)
	defer func() {
		_ = db.Close()
	}()

	require.Nil(t, db.Put([]byte("key"), []byte("value")))
}

func TestDB_GetErrorAfterPutBeforeTimeout(t *testing.T) {
	t.Parallel()

	db := createPebbleDB(t, 10, 100)
	defer func() {
		_ = db.Close()
	}()

	key, val := []byte("key"), []byte("value")
	require.Nil(t, db.Put(key, val))

	res, err := db.Get(key)
	require.Nil(t, err)
	require.Equal(t, val, res)
}

func TestDB_GetOKAfterPutWithTimeout(t *testing.T) {
	t.Parallel()

	db := createPebbleDB(t, 1, 100)
	defer func() {
		_ = db.Close()
	}()

	key, val := []byte("key"), []byte("value")
	require.Nil(t, db.Put(key, val))
	time.Sleep(time.Second * 2)

	res, err := db.Get(key)
	require.Nil(t, err)
	require.Equal(t, val, res)
}

func TestDB_RemoveBeforeTimeoutOK(t *testing.T) {
	t.Parallel()

	db := createPebbleDB(t, 10, 100)
	defer func() {
		_ = db.Close()
	}()

	key, val := []byte("key"), []byte("value")
	require.Nil(t, db.Put(key, val))
	require.Nil(t, db.Remove(key))

	res, err := db.Get(key)
	require.Nil(t, res)
	require.Equal(t, common.ErrKeyNotFound, err)
	require.Equal(t, common.ErrKeyNotFound, db.Has(key))
}

func TestDB_RemoveAfterTimeoutOK(t *testing.T) {
	t.Parallel()

	db := createPebbleDB(t, 1, 100)
	defer func() {
		_ = db.Close()
	}()

	key, val := []byte("key"), []byte("value")
	require.Nil(t, db.Put(key, val))
	time.Sleep(time.Second * 2)
	require.Nil(t, db.Remove(key))
	time.Sleep(time.Second * 2)

	res, err := db.Get(key)
	require.Nil(t, res)
	require.Equal(t, common.ErrKeyNotFound, err)
}

func TestDB_GetHasPresentAndNotPresent(t *testing.T) {
	t.Parallel()

	db := createPebbleDB(t, 10, 1)
	defer func() {
		_ = db.Close()
	}()

	key, val := []byte("key"), []byte("value")
	require.Nil(t, db.Put(key, val))

	res, err := db.Get(key)
	require.Nil(t, err)
	require.Equal(t, val, res)
	require.Nil(t, db.Has(key))

	res, err = db.Get([]byte("missing"))
	require.Nil(t, res)
	require.Equal(t, common.ErrKeyNotFound, err)
	require.Equal(t, common.ErrKeyNotFound, db.Has([]byte("missing")))
}

func TestDB_RemoveNotPresent(t *testing.T) {
	t.Parallel()

	db := createPebbleDB(t, 10, 1)
	defer func() {
		_ = db.Close()
	}()

	require.Nil(t, db.Remove([]byte("missing")))
}

func TestDB_EmptyValueIsStored(t *testing.T) {
	t.Parallel()

	db := createPebbleDB(t, 10, 100)
	defer func() {
		_ = db.Close()
	}()

	key := []byte("key")
	require.Nil(t, db.Put(key, []byte{}))

	res, err := db.Get(key)
	require.Nil(t, err)
	require.Empty(t, res)
	require.Nil(t, db.Has(key))
}

func TestDB_CallerBuffersAreNeverAliased(t *testing.T) {
	t.Parallel()

	db := createPebbleDB(t, 10, 100)
	defer func() {
		_ = db.Close()
	}()

	key := []byte("key")
	val := []byte("value")
	require.Nil(t, db.Put(key, val))
	val[0] = 'X'

	buffered, err := db.Get(key)
	require.Nil(t, err)
	require.Equal(t, []byte("value"), buffered)
	buffered[0] = 'Y'

	buffered, err = db.Get(key)
	require.Nil(t, err)
	require.Equal(t, []byte("value"), buffered)
}

func TestDB_CloseTwiceAndDestroy(t *testing.T) {
	t.Parallel()

	t.Run("close twice", func(t *testing.T) {
		t.Parallel()

		db := createPebbleDB(t, 10, 1)
		require.Nil(t, db.Close())
		require.Nil(t, db.Close())
	})
	t.Run("destroy removes the directory", func(t *testing.T) {
		t.Parallel()

		path := t.TempDir() + "/db"
		db, err := pebbledb.NewDB(createArgs(path, 10, 1))
		require.Nil(t, err)
		require.Nil(t, db.Put([]byte("key"), []byte("value")))

		require.Nil(t, db.Destroy())

		_, err = os.Stat(path)
		require.True(t, os.IsNotExist(err))
	})
	t.Run("destroy closed removes the directory", func(t *testing.T) {
		t.Parallel()

		path := t.TempDir() + "/db"
		db, err := pebbledb.NewDB(createArgs(path, 10, 1))
		require.Nil(t, err)
		require.Nil(t, db.Close())

		require.Nil(t, db.DestroyClosed())

		_, err = os.Stat(path)
		require.True(t, os.IsNotExist(err))
	})
}

func TestDB_MethodCallsAfterCloseOrDestroy(t *testing.T) {
	t.Parallel()

	t.Run("when closing", func(t *testing.T) {
		t.Parallel()

		testAllMethodsShouldNotPanic(t, func(db *pebbledb.DB) {
			_ = db.Close()
		})
	})
	t.Run("when destroying", func(t *testing.T) {
		t.Parallel()

		testAllMethodsShouldNotPanic(t, func(db *pebbledb.DB) {
			_ = db.Destroy()
		})
	})
}

func testAllMethodsShouldNotPanic(t *testing.T, closeHandler func(db *pebbledb.DB)) {
	db := createPebbleDB(t, 10, 1)

	defer func() {
		r := recover()
		if r != nil {
			assert.Fail(t, fmt.Sprintf("should have not panic %v", r))
		}
	}()

	closeHandler(db)

	_, err := db.Get([]byte("key1"))
	assert.Equal(t, common.ErrDBIsClosed, err)

	err = db.Has([]byte("key2"))
	assert.Equal(t, common.ErrDBIsClosed, err)

	err = db.Remove([]byte("key3"))
	assert.Equal(t, common.ErrDBIsClosed, err)

	err = db.Put([]byte("key4"), []byte("val"))
	assert.Equal(t, common.ErrDBIsClosed, err)

	db.RangeKeys(func(key []byte, value []byte) bool {
		require.Fail(t, "should have not called range")
		return false
	})
}

func TestDB_DataSurvivesReopen(t *testing.T) {
	t.Parallel()

	path := t.TempDir()
	db, err := pebbledb.NewDB(createArgs(path, 10, 100))
	require.Nil(t, err)

	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		require.Nil(t, db.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i))))
	}
	require.Nil(t, db.Remove([]byte("key7")))
	require.Nil(t, db.Close())

	db, err = pebbledb.NewDB(createArgs(path, 10, 100))
	require.Nil(t, err)
	defer func() {
		_ = db.Close()
	}()

	for i := 0; i < numKeys; i++ {
		res, errGet := db.Get([]byte(fmt.Sprintf("key%d", i)))
		if i == 7 {
			require.Equal(t, common.ErrKeyNotFound, errGet)
			continue
		}
		require.Nil(t, errGet)
		require.Equal(t, []byte(fmt.Sprintf("value%d", i)), res)
	}
}

func TestDB_RangeKeys(t *testing.T) {
	t.Parallel()

	db := createPebbleDB(t, 10, 1)
	defer func() {
		_ = db.Close()
	}()

	expected := map[string]string{"a": "1", "b": "2", "c": "3"}
	for key, val := range expected {
		require.Nil(t, db.Put([]byte(key), []byte(val)))
	}

	found := make(map[string]string)
	db.RangeKeys(func(key []byte, value []byte) bool {
		found[string(key)] = string(value)
		return true
	})
	require.Equal(t, expected, found)

	numCalls := 0
	db.RangeKeys(func(key []byte, value []byte) bool {
		numCalls++
		return false
	})
	require.Equal(t, 1, numCalls)

	db.RangeKeys(nil)
}

func TestDB_SpecialValueTest(t *testing.T) {
	t.Parallel()

	db := createPebbleDB(t, 100, 100)
	key := []byte("key")
	removedValue := []byte("removed")
	randomValue := []byte("random")
	t.Run("operations: put -> get of 'removed' value", func(t *testing.T) {
		require.Nil(t, db.Put(key, removedValue))

		recovered, err := db.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, removedValue, recovered)
	})
	t.Run("operations: put -> remove -> get of 'removed' value", func(t *testing.T) {
		require.Nil(t, db.Put(key, removedValue))
		require.Nil(t, db.Remove(key))

		recovered, err := db.Get(key)
		assert.Equal(t, common.ErrKeyNotFound, err)
		assert.Nil(t, recovered)
	})
	t.Run("operations: put -> remove -> put -> get of 'removed' value", func(t *testing.T) {
		require.Nil(t, db.Put(key, removedValue))
		require.Nil(t, db.Remove(key))
		require.Nil(t, db.Put(key, removedValue))

		recovered, err := db.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, removedValue, recovered)
	})
	t.Run("operations: put -> remove -> put -> get of random value", func(t *testing.T) {
		require.Nil(t, db.Put(key, randomValue))
		require.Nil(t, db.Remove(key))
		require.Nil(t, db.Put(key, randomValue))

		recovered, err := db.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, randomValue, recovered)
	})

	_ = db.Close()
}

func TestDB_SharedResources(t *testing.T) {
	t.Parallel()

	t.Run("invalid sizes should fail", func(t *testing.T) {
		t.Parallel()

		resources, err := pebbledb.NewSharedResources(0, 10)
		require.Nil(t, resources)
		require.ErrorIs(t, err, common.ErrInvalidConfig)

		resources, err = pebbledb.NewSharedResources(1024, 0)
		require.Nil(t, resources)
		require.ErrorIs(t, err, common.ErrInvalidConfig)
	})
	t.Run("close on nil should not panic", func(t *testing.T) {
		t.Parallel()

		var resources *pebbledb.SharedResources
		require.True(t, resources.IsInterfaceNil())
		resources.Close()
	})
	t.Run("two persisters share the caches and outlive their release", func(t *testing.T) {
		t.Parallel()

		resources, err := pebbledb.NewSharedResources(8<<20, 100)
		require.Nil(t, err)
		require.False(t, resources.IsInterfaceNil())

		args1 := createArgs(t.TempDir(), 10, 1)
		args1.SharedResources = resources
		db1, err := pebbledb.NewDB(args1)
		require.Nil(t, err)

		args2 := createArgs(t.TempDir(), 10, 1)
		args2.SharedResources = resources
		db2, err := pebbledb.NewDB(args2)
		require.Nil(t, err)

		key, val := []byte("key"), []byte("value")
		require.Nil(t, db1.Put(key, val))
		require.Nil(t, db2.Put(key, val))

		// releasing the creator's reference first must keep the caches alive for the open persisters
		resources.Close()
		resources.Close()

		res, err := db1.Get(key)
		require.Nil(t, err)
		require.Equal(t, val, res)
		res, err = db2.Get(key)
		require.Nil(t, err)
		require.Equal(t, val, res)

		require.Nil(t, db1.Close())
		require.Nil(t, db2.Close())
	})
}

func TestDB_ConcurrentOperations(t *testing.T) {
	t.Parallel()

	db := createPebbleDB(t, 1, 10)

	numOps := 300
	wg := sync.WaitGroup{}
	wg.Add(numOps)

	for i := 0; i < numOps; i++ {
		go func(idx int) {
			modRes := idx % 9
			testKey := []byte(fmt.Sprintf("%d", modRes))
			testVal := testKey
			switch modRes {
			case 0:
				_ = db.Close()
			case 1:
				_ = db.Destroy()
			case 2:
				_ = db.DestroyClosed()
			case 3:
				_, _ = db.Get(testKey)
			case 4:
				_ = db.Has(testKey)
			case 5:
				db.IsInterfaceNil()
			case 6:
				_ = db.Put(testKey, testVal)
			case 7:
				db.RangeKeys(func(key []byte, value []byte) bool {
					return true
				})
			case 8:
				_ = db.Remove(testKey)
			}

			wg.Done()
		}(i)
	}

	wg.Wait()
}

func TestDB_ConcurrentReadersAndWriters(t *testing.T) {
	t.Parallel()

	db := createPebbleDB(t, 1, 50)
	defer func() {
		_ = db.Close()
	}()

	numWriters := 8
	numKeysPerWriter := 500
	wg := sync.WaitGroup{}
	wg.Add(numWriters * 2)
	for w := 0; w < numWriters; w++ {
		go func(writer int) {
			defer wg.Done()
			for i := 0; i < numKeysPerWriter; i++ {
				key := []byte(fmt.Sprintf("w%d-k%d", writer, i))
				require.Nil(t, db.Put(key, key))
			}
		}(w)
		go func(writer int) {
			defer wg.Done()
			for i := 0; i < numKeysPerWriter; i++ {
				key := []byte(fmt.Sprintf("w%d-k%d", writer, i))
				res, err := db.Get(key)
				if err == nil {
					require.Equal(t, key, res)
				} else {
					require.Equal(t, common.ErrKeyNotFound, err)
				}
			}
		}(w)
	}
	wg.Wait()

	for w := 0; w < numWriters; w++ {
		for i := 0; i < numKeysPerWriter; i++ {
			key := []byte(fmt.Sprintf("w%d-k%d", w, i))
			res, err := db.Get(key)
			require.Nil(t, err)
			require.Equal(t, key, res)
		}
	}
}

func BenchmarkDB_PutGet(b *testing.B) {
	db := createPebbleDB(b, 10000, 10000000)
	defer func() {
		_ = db.Close()
	}()
	key := []byte("key")
	value := []byte("value")

	b.Run("put -> get same key", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = db.Put(key, value)
			_, _ = db.Get(key)
		}
	})
	b.Run("put -> remove -> get with different keys", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			testKey := append(key, big.NewInt(int64(i)).Bytes()...)

			_ = db.Put(testKey, value)
			_ = db.Remove(testKey)
			_, _ = db.Get(testKey)
		}
	})
}
