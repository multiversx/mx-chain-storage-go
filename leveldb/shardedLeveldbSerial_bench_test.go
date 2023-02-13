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

func BenchmarkPersisterPutAllKeys(b *testing.B) {
	b.Run("1 mil keys", func(b *testing.B) {
		putKeysBenchmarkByNumKeys(b, _1Mil)
	})
	b.Run("2 mil keys", func(b *testing.B) {
		putKeysBenchmarkByNumKeys(b, _1Mil)
	})
	b.Run("4 mil keys", func(b *testing.B) {
		putKeysBenchmarkByNumKeys(b, _1Mil)
	})
}

func putKeysBenchmarkByNumKeys(
	b *testing.B,
	numKeys int,
) {
	entries, _ := generateKeys(numKeys)

	persisterPath := b.TempDir()
	singleDB, err := createPersister(persisterPath, singleID)
	require.Nil(b, err)
	defer singleDB.Close()

	shardedPersisterPath := b.TempDir()
	shardedDB, err := createPersister(shardedPersisterPath, shardedID)
	require.Nil(b, err)
	defer shardedDB.Close()

	b.Run("persister", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			putKeys(b, singleDB, entries)
		}
	})

	b.Run("sharded persister", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			putKeys(b, shardedDB, entries)
		}
	})
}

func putKeys(
	b *testing.B,
	db types.Persister,
	entries map[string][]byte,
) {
	maxRoutines := make(chan struct{}, 400)
	wg := sync.WaitGroup{}
	wg.Add(len(entries))

	for key, val := range entries {
		maxRoutines <- struct{}{}
		go func(key, val []byte) {
			err := db.Put(key, val)
			require.Nil(b, err)

			<-maxRoutines
			wg.Done()
		}([]byte(key), val)
	}

	wg.Wait()
}

func BenchmarkPersisterCopyAllKeys(b *testing.B) {
	b.Run("1 mil keys", func(b *testing.B) {
		copyKeysBenchmarkByNumKeys(b, _1Mil)
	})
	b.Run("2 mil keys", func(b *testing.B) {
		copyKeysBenchmarkByNumKeys(b, _1Mil)
	})
}

func copyKeysBenchmarkByNumKeys(b *testing.B, numKeys int) {
	entries, _ := generateKeys(_1Mil)

	persisterPath := b.TempDir()
	singleDB, err := createPersister(persisterPath, singleID)
	require.Nil(b, err)
	err = populatePersister(singleDB, entries)
	require.Nil(b, err)
	defer singleDB.Close()

	shardedPersisterPath := b.TempDir()
	shardedDB, err := createPersister(shardedPersisterPath, shardedID)
	require.Nil(b, err)
	err = populatePersister(shardedDB, entries)
	require.Nil(b, err)
	defer shardedDB.Close()

	singleDBNew, err := createPersister(b.TempDir(), singleID)
	require.Nil(b, err)
	defer singleDB.Close()

	shardedDBNew, err := createPersister(b.TempDir(), shardedID)
	require.Nil(b, err)
	defer shardedDB.Close()

	b.Run("persister", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			copyKeys(b, entries, singleDB, singleDBNew)
		}
	})

	b.Run("sharded persister", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			copyKeys(b, entries, shardedDB, shardedDBNew)
		}
	})
}

func copyKeys(
	b *testing.B,
	entries map[string][]byte,
	oldDB types.Persister,
	newDB types.Persister,
) {
	maxRoutines := make(chan struct{}, 400)
	wg := sync.WaitGroup{}
	wg.Add(len(entries))

	for key, val := range entries {
		maxRoutines <- struct{}{}
		go func(key, val []byte) {
			_, err := oldDB.Get(key)
			require.Nil(b, err)

			err = newDB.Put(key, val)
			require.Nil(b, err)

			<-maxRoutines
			wg.Done()
		}([]byte(key), val)
	}

	wg.Wait()
}

func BenchmarkPersister1milGetAllKeys(b *testing.B) {
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

func BenchmarkPersister1milGet10kKeys(b *testing.B) {
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

func BenchmarkPersister1milGet100kKeys(b *testing.B) {
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

func BenchmarkPersister2milGetAllKeys(b *testing.B) {
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

func BenchmarkPersister4milGetAllKeys(b *testing.B) {
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

func BenchmarkPersister8milGetAllKeys(b *testing.B) {
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
		return leveldb.NewSerialDB(path, 2, _1Mil, 10)
	case shardedID:
		shardCoordinator, _ := leveldb.NewShardIDProvider(numShards)
		return leveldb.NewShardedPersister(path, 2, _1Mil, 10, shardCoordinator)
	default:
		return nil, fmt.Errorf("failed to create persister: invalid id type")
	}
}

func populatePersister(db types.Persister, entries map[string][]byte) error {
	for key, val := range entries {
		err := db.Put([]byte(key), val)
		if err != nil {
			return err
		}
	}

	return nil
}

func createAndPopulatePersister(path string, id string, entries map[string][]byte) error {
	db, err := createPersister(path, id)
	if err != nil {
		return err
	}

	err = populatePersister(db, entries)
	if err != nil {
		return err
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
