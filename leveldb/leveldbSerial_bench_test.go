package leveldb

import (
	"crypto/rand"
	"errors"
	"testing"

	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	_1Mil = 1_000_000
	_1KB  = 1_024
)

func BenchmarkLevelDBBloomMiss(b *testing.B) {
	_, keysForDB := generateKeys(_1Mil)
	_, missingKeys := generateKeys(5 * _1KB)

	bigValue := generateRandomByteArray(5_000)

	runBenchmark := func(b *testing.B, bloomBits int) {
		persisterPath := b.TempDir()

		db, err := NewSerialDB(persisterPath, 2, _1Mil, 10, bloomBits)
		if err != nil {
			b.Fatalf("createPersister failed: %v", err)
		}
		defer db.Close()

		// populate
		for idx := 0; idx < _1Mil; idx++ {
			if err = db.Put([]byte(keysForDB[idx]), bigValue); err != nil {
				b.Fatalf("put failed: %v", err)
			}
		}

		err = db.db.CompactRange(util.Range{Start: nil, Limit: nil})
		if err != nil {
			b.Fatalf("compact failed: %v", err)
		}

		// reset timer AFTER setup
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := missingKeys[i%len(missingKeys)]
			_, err = db.Get([]byte(key))
			if !errors.Is(err, common.ErrKeyNotFound) {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	}

	b.Run("single shard no bloom filter", func(b *testing.B) {
		runBenchmark(b, 0)
	})
	b.Run("single shard with bloom filter", func(b *testing.B) {
		runBenchmark(b, 10)
	})
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
