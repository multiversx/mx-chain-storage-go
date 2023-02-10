package leveldb_test

import (
	"crypto/rand"
	"fmt"
	"sync"
	"testing"

	"github.com/multiversx/mx-chain-storage-go/leveldb"
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

func generateRandomByteArray(size int) []byte {
	r := make([]byte, size)
	_, _ = rand.Read(r)
	return r
}
