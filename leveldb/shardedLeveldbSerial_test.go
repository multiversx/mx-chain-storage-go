package leveldb_test

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"testing"

	"github.com/multiversx/mx-chain-storage-go/leveldb"
	"github.com/multiversx/mx-chain-storage-go/testscommon"
	"github.com/multiversx/mx-chain-storage-go/types"
	"github.com/stretchr/testify/require"
)

const (
	_1Mil = 1_000_000
	_2Mil = 2_000_000
	_4Mil = 4_000_000
	_8Mil = 8_000_000
	_10k  = 10_000
	_100k = 100_000

	numShards = 4

	singleID  = "single"
	shardedID = "sharded"
)

func TestNewShardedPersister(t *testing.T) {
	t.Parallel()

	t.Run("nil id provider", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		db, err := leveldb.NewShardedPersister(dir, 2, 10, 10, nil)
		require.Nil(t, db)
		require.Equal(t, leveldb.ErrNilIDProvider, err)
	})

	t.Run("should work", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		db, err := leveldb.NewShardedPersister(dir, 2, 10, 10, &testscommon.ShardIDProviderStub{})
		require.NotNil(t, db)
		require.Nil(t, err)
	})
}

func TestShardedPersister_Operations(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := createPersister(dir, shardedID)
	require.Nil(t, err)

	_ = db.Put([]byte("aaa"), []byte("aaaval"))
	_ = db.Put([]byte("aab"), []byte("aabval"))
	_ = db.Put([]byte("aac"), []byte("aacval"))

	err = db.Close()
	require.Nil(t, err)

	db2, err := createPersister(dir, shardedID)
	require.Nil(t, err)

	_, err = db2.Get([]byte("aaa"))
	require.Nil(t, err)

	_, err = db2.Get([]byte("aab"))
	require.Nil(t, err)

	_, err = db2.Get([]byte("aac"))
	require.Nil(t, err)

	require.Nil(t, db2.Has([]byte("aaa")))

	err = db2.Remove([]byte("aaa"))
	require.Nil(t, err)

	require.NotNil(t, db2.Has([]byte("aaa")))

	numKeys := 0
	db2.RangeKeys(func(key []byte, val []byte) bool {
		numKeys++
		return true
	})

	expNumKeys := 3
	require.Equal(t, expNumKeys, numKeys)

	err = db2.Close()
	require.Nil(t, err)

	err = db2.DestroyClosed()
	require.Nil(t, err)

}

// ---------- Benchmarks ----------

func BenchmarkPersisterOneKey1mil(b *testing.B) {
	entries, keys := generateKeys(_1Mil)
	randIndex := randInt(0, len(keys)-1)
	key := []byte(keys[randIndex])

	persisterPath := b.TempDir()
	_ = createAndPopulatePersister(persisterPath, singleID, entries)
	shardedPersisterPath := b.TempDir()
	_ = createAndPopulatePersister(shardedPersisterPath, shardedID, entries)

	b.Run("persister", func(b *testing.B) {
		benchmarkPersisterOneKey(b, key, persisterPath, singleID)
	})
	b.Run("sharded persister", func(b *testing.B) {
		benchmarkPersisterOneKey(b, key, shardedPersisterPath, shardedID)
	})
}

func BenchmarkPersisterOneKey2mil(b *testing.B) {
	entries, keys := generateKeys(_2Mil)
	randIndex := randInt(0, len(keys)-1)
	key := []byte(keys[randIndex])

	persisterPath := b.TempDir()
	_ = createAndPopulatePersister(persisterPath, singleID, entries)
	shardedPersisterPath := b.TempDir()
	_ = createAndPopulatePersister(shardedPersisterPath, shardedID, entries)

	b.Run("persister", func(b *testing.B) {
		benchmarkPersisterOneKey(b, key, persisterPath, singleID)
	})

	b.Run("sharded persister", func(b *testing.B) {
		benchmarkPersisterOneKey(b, key, shardedPersisterPath, shardedID)
	})
}

func BenchmarkPersisterOneKey4mil(b *testing.B) {
	entries, keys := generateKeys(_4Mil)
	randIndex := randInt(0, len(keys)-1)
	key := []byte(keys[randIndex])

	persisterPath := b.TempDir()
	_ = createAndPopulatePersister(persisterPath, singleID, entries)
	shardedPersisterPath := b.TempDir()
	_ = createAndPopulatePersister(shardedPersisterPath, shardedID, entries)

	b.Run("persister", func(b *testing.B) {
		benchmarkPersisterOneKey(b, key, persisterPath, singleID)
	})

	b.Run("sharded persister", func(b *testing.B) {
		benchmarkPersisterOneKey(b, key, shardedPersisterPath, shardedID)
	})
}

func BenchmarkPersisterOneKey8mil(b *testing.B) {
	entries, keys := generateKeys(_8Mil)
	randIndex := randInt(0, len(keys)-1)
	key := []byte(keys[randIndex])

	persisterPath := b.TempDir()
	_ = createAndPopulatePersister(persisterPath, singleID, entries)
	shardedPersisterPath := b.TempDir()
	_ = createAndPopulatePersister(shardedPersisterPath, shardedID, entries)

	b.Run("persister", func(b *testing.B) {
		benchmarkPersisterOneKey(b, key, persisterPath, singleID)
	})

	b.Run("sharded persister", func(b *testing.B) {
		benchmarkPersisterOneKey(b, key, shardedPersisterPath, shardedID)
	})
}

func BenchmarkPersister1milAllKeys(b *testing.B) {
	entries, keys := generateKeys(_1Mil)

	persisterPath := b.TempDir()
	err := createAndPopulatePersister(persisterPath, singleID, entries)
	require.Nil(b, err)
	shardedPersisterPath := b.TempDir()
	err = createAndPopulatePersister(shardedPersisterPath, shardedID, entries)
	require.Nil(b, err)

	b.Run("persister", func(b *testing.B) {
		benchmarkPersister(b, keys, persisterPath, singleID)
	})

	b.Run("sharded persister", func(b *testing.B) {
		benchmarkPersister(b, keys, shardedPersisterPath, shardedID)
	})
}

func BenchmarkPersister1mil10kKeys(b *testing.B) {
	entries, keys := generateKeys(_1Mil)

	persisterPath := b.TempDir()
	_ = createAndPopulatePersister(persisterPath, singleID, entries)
	shardedPersisterPath := b.TempDir()
	_ = createAndPopulatePersister(shardedPersisterPath, shardedID, entries)

	b.Run("persister", func(b *testing.B) {
		benchmarkPersister(b, keys[0:_10k], persisterPath, singleID)
	})

	b.Run("sharded persister", func(b *testing.B) {
		benchmarkPersister(b, keys[0:_10k], shardedPersisterPath, shardedID)
	})
}

func BenchmarkPersister1mil100kKeys(b *testing.B) {
	entries, keys := generateKeys(_1Mil)

	persisterPath := b.TempDir()
	_ = createAndPopulatePersister(persisterPath, singleID, entries)
	shardedPersisterPath := b.TempDir()
	_ = createAndPopulatePersister(shardedPersisterPath, shardedID, entries)

	b.Run("persister", func(b *testing.B) {
		benchmarkPersister(b, keys[0:_100k], persisterPath, singleID)
	})

	b.Run("sharded persister", func(b *testing.B) {
		benchmarkPersister(b, keys[0:_100k], shardedPersisterPath, shardedID)
	})
}

func BenchmarkPersister2milAllKeys(b *testing.B) {
	entries, keys := generateKeys(_2Mil)

	persisterPath := b.TempDir()
	_ = createAndPopulatePersister(persisterPath, singleID, entries)
	shardedPersisterPath := b.TempDir()
	_ = createAndPopulatePersister(shardedPersisterPath, shardedID, entries)

	b.Run("persister", func(b *testing.B) {
		benchmarkPersister(b, keys, persisterPath, singleID)
	})

	b.Run("sharded persister", func(b *testing.B) {
		benchmarkPersister(b, keys, shardedPersisterPath, shardedID)
	})
}

func BenchmarkPersister4milAllKeys(b *testing.B) {
	entries, keys := generateKeys(_4Mil)

	persisterPath := b.TempDir()
	_ = createAndPopulatePersister(persisterPath, singleID, entries)
	shardedPersisterPath := b.TempDir()
	_ = createAndPopulatePersister(shardedPersisterPath, shardedID, entries)

	b.Run("persister", func(b *testing.B) {
		benchmarkPersister(b, keys, persisterPath, singleID)
	})

	b.Run("sharded persister", func(b *testing.B) {
		benchmarkPersister(b, keys, shardedPersisterPath, shardedID)
	})
}

func BenchmarkPersister8milAllKeys(b *testing.B) {
	entries, keys := generateKeys(_8Mil)

	persisterPath := b.TempDir()
	_ = createAndPopulatePersister(persisterPath, singleID, entries)
	shardedPersisterPath := b.TempDir()
	_ = createAndPopulatePersister(shardedPersisterPath, shardedID, entries)

	b.Run("persister", func(b *testing.B) {
		benchmarkPersister(b, keys, persisterPath, singleID)
	})

	b.Run("sharded persister", func(b *testing.B) {
		benchmarkPersister(b, keys, shardedPersisterPath, shardedID)
	})
}

type persister interface {
}

func createPersister(path string, id string) (types.Persister, error) {
	switch id {
	case singleID:
		return leveldb.NewSerialDB(path, 2, 1000, 10)
	case shardedID:
		shardCoordinator, _ := leveldb.NewShardIDProvider(numShards)
		return leveldb.NewShardedPersister(path, 2, 1000, 10, shardCoordinator)
	default:
		return nil, fmt.Errorf("failed to create persister: invalid id type")
	}
}

func createAndPopulatePersister(path string, id string, entries map[string][]byte) error {
	db, err := createPersister(path, id)
	if err != nil {
		return err
	}

	for key, val := range entries {
		err = db.Put([]byte(key), val)
		if err != nil {
			return err
		}
	}

	err = db.Close()
	if err != nil {
		return err
	}

	return nil
}

func benchmarkPersisterOneKey(b *testing.B, key []byte, path string, id string) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getKey(b, key, path, id)
	}
}

func benchmarkPersister(
	b *testing.B,
	keys []string,
	path string,
	id string,
) {
	for i := 0; i < b.N; i++ {
		getKeys(b, keys, path, id)
	}
}

func getKeys(
	b *testing.B,
	keys []string,
	path string,
	id string,
) {
	db := createPersisterWithTimerControl(b, path, id)

	defer func() {
		closePersisterWithTimerControl(b, db)
	}()

	maxRoutines := make(chan struct{}, 400)
	wg := sync.WaitGroup{}
	wg.Add(len(keys))

	for _, key := range keys {
		maxRoutines <- struct{}{}
		go func(key string) {
			_, err := db.Get([]byte(key))
			require.Nil(b, err)

			<-maxRoutines
			wg.Done()
		}(key)
	}

	wg.Wait()
}

func getKey(
	b *testing.B,
	key []byte,
	path string,
	id string,
) {
	db := createPersisterWithTimerControl(b, path, id)

	defer func() {
		closePersisterWithTimerControl(b, db)
	}()

	_, err := db.Get(key)
	require.Nil(b, err)
}

func createPersisterWithTimerControl(b *testing.B, path, id string) types.Persister {
	b.StopTimer()
	db, err := createPersister(path, id)
	require.Nil(b, err)
	b.StartTimer()

	return db
}

func closePersisterWithTimerControl(b *testing.B, db types.Persister) {
	b.StopTimer()
	err := db.Close()
	require.Nil(b, err)
	b.StartTimer()
}

func generateKeys(numKeys int) (map[string][]byte, []string) {
	entries := make(map[string][]byte)

	keys := make([]string, 0)

	for i := 0; i < numKeys; i++ {
		key := generateRandomByteArray(32)
		value := generateRandomByteArray(32)

		entries[string(key)] = value
		keys = append(keys, string(key))
	}

	return entries, keys
}

func randInt(min int, max int) int {
	dd := int64(max - min)
	vv, _ := rand.Int(rand.Reader, big.NewInt(dd))
	return min + int(vv.Int64())
}

func generateRandomByteArray(size int) []byte {
	r := make([]byte, size)
	_, _ = rand.Read(r)
	return r
}
