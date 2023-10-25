package storageUnit_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/lrucache"
	"github.com/multiversx/mx-chain-storage-go/memorydb"
	"github.com/multiversx/mx-chain-storage-go/storageUnit"
	"github.com/multiversx/mx-chain-storage-go/testscommon"
	"github.com/multiversx/mx-chain-storage-go/types"
	"github.com/stretchr/testify/assert"
)

func logError(err error) {
	if err != nil {
		fmt.Println(err.Error())
	}
}

func initStorageUnit(tb testing.TB, cSize int) *storageUnit.Unit {
	mdb := memorydb.New()
	cache, err2 := lrucache.NewCache(cSize)
	assert.Nil(tb, err2, "no error expected but got %s", err2)

	sUnit, err := storageUnit.NewStorageUnit(cache, mdb)
	assert.Nil(tb, err, "failed to create storage unit")

	return sUnit
}

func createCacheWithRemovalTracking() types.Cacher {
	cacher, _ := storageUnit.NewCache(storageUnit.CacheCreationConfig{
		CacheConfig: storageUnit.CacheConfig{
			Capacity: 10,
			Type:     storageUnit.LRUCache,
		},
		RemovalTrackingCacheConfig: storageUnit.CacheConfig{
			Capacity: 10,
			Type:     storageUnit.LRUCache,
		},
	})

	return cacher
}

func TestStorageUnitNilPersister(t *testing.T) {
	t.Parallel()

	cache, err1 := lrucache.NewCache(10)

	assert.Nil(t, err1, "no error expected but got %s", err1)

	_, err := storageUnit.NewStorageUnit(cache, nil)

	assert.NotNil(t, err, "expected failure")
}

func TestStorageUnitNilCacher(t *testing.T) {
	t.Parallel()

	mdb := memorydb.New()

	_, err1 := storageUnit.NewStorageUnit(nil, mdb)
	assert.NotNil(t, err1, "expected failure")
}

func TestStorageUnit(t *testing.T) {
	t.Parallel()

	cache, err1 := lrucache.NewCache(10)
	mdb := memorydb.New()

	assert.Nil(t, err1, "no error expected but got %s", err1)

	_, err := storageUnit.NewStorageUnit(cache, mdb)
	assert.Nil(t, err, "did not expect failure")
}

func TestPutNotPresent(t *testing.T) {
	t.Parallel()

	key, val := []byte("key0"), []byte("value0")
	s := initStorageUnit(t, 10)
	err := s.Put(key, val)

	assert.Nil(t, err, "no error expected but got %s", err)

	err = s.Has(key)

	assert.Nil(t, err, "no error expected but got %s", err)
}

func TestPutNotPresentCache(t *testing.T) {
	t.Parallel()

	key, val := []byte("key1"), []byte("value1")
	s := initStorageUnit(t, 10)
	err := s.Put(key, val)

	assert.Nil(t, err, "no error expected but got %s", err)

	s.ClearCache()

	err = s.Has(key)

	assert.Nil(t, err, "no error expected but got %s", err)
}

func TestPutPresentShouldOverwriteValue(t *testing.T) {
	t.Parallel()

	key, val := []byte("key2"), []byte("value2")
	s := initStorageUnit(t, 10)
	err := s.Put(key, val)

	assert.Nil(t, err, "no error expected but got %s", err)

	newVal := []byte("value5")
	err = s.Put(key, newVal)
	assert.Nil(t, err, "no error expected but got %s", err)

	returnedVal, err := s.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, newVal, returnedVal)
}

func TestGetNotPresent(t *testing.T) {
	t.Parallel()

	key := []byte("key3")
	s := initStorageUnit(t, 10)
	v, err := s.Get(key)

	assert.NotNil(t, err, "expected to find no value, but found %s", v)
	assert.Nil(t, v)
}

func TestUnit_GetWithExplicitlyRemovedKeyShouldNotCallThePersister(t *testing.T) {
	t.Parallel()

	mdb := &testscommon.PersisterStub{
		HasCalled: func(key []byte) error {
			assert.Fail(t, "should have not called Has")
			return nil
		},
		GetCalled: func(key []byte) ([]byte, error) {
			assert.Fail(t, "should have not called Get")
			return nil, nil
		},
	}
	cache := createCacheWithRemovalTracking()
	storer, err := storageUnit.NewStorageUnit(cache, mdb)
	assert.Nil(t, err)

	key := []byte("key")
	value := []byte("value")

	_ = storer.Put(key, value)
	_ = storer.Remove(key)

	v, err := storer.Get(key)
	assert.Equal(t, err, common.ErrKeyNotFound) // same as the old behaviour (key not present in the cacher & persister)
	assert.Nil(t, v)                            // same as the old behaviour (key not present in the cacher & persister)
}

func TestGetNotPresentCache(t *testing.T) {
	t.Parallel()

	key, val := []byte("key4"), []byte("value4")
	s := initStorageUnit(t, 10)
	err := s.Put(key, val)

	assert.Nil(t, err, "no error expected but got %s", err)

	s.ClearCache()

	v, err := s.Get(key)

	assert.Nil(t, err, "expected no error, but got %s", err)
	assert.Equal(t, val, v, "expected %s but got %s", val, v)
}

func TestGetPresent(t *testing.T) {
	t.Parallel()

	key, val := []byte("key5"), []byte("value4")
	s := initStorageUnit(t, 10)
	err := s.Put(key, val)

	assert.Nil(t, err, "no error expected but got %s", err)

	v, err := s.Get(key)

	assert.Nil(t, err, "expected no error, but got %s", err)
	assert.Equal(t, val, v, "expected %s but got %s", val, v)
}

func TestHasNotPresent(t *testing.T) {
	t.Parallel()

	key := []byte("key6")
	s := initStorageUnit(t, 10)
	err := s.Has(key)

	assert.NotNil(t, err)
	assert.Equal(t, err, common.ErrKeyNotFound)
}

func TestUnit_HasWithExplicitlyRemovedKeyShouldNotCallThePersister(t *testing.T) {
	t.Parallel()

	mdb := &testscommon.PersisterStub{
		HasCalled: func(key []byte) error {
			assert.Fail(t, "should have not called Has")
			return nil
		},
		GetCalled: func(key []byte) ([]byte, error) {
			assert.Fail(t, "should have not called Get")
			return nil, nil
		},
	}
	cache := createCacheWithRemovalTracking()
	storer, err := storageUnit.NewStorageUnit(cache, mdb)
	assert.Nil(t, err)

	key := []byte("key")
	value := []byte("value")

	_ = storer.Put(key, value)
	_ = storer.Remove(key)

	err = storer.Has(key)
	assert.Equal(t, err, common.ErrKeyNotFound) // same as the old behaviour (key not present in the cacher & persister)
}

func TestHasNotPresentCache(t *testing.T) {
	t.Parallel()

	key, val := []byte("key7"), []byte("value7")
	s := initStorageUnit(t, 10)
	err := s.Put(key, val)

	assert.Nil(t, err, "no error expected but got %s", err)

	s.ClearCache()

	err = s.Has(key)

	assert.Nil(t, err, "expected no error, but got %s", err)
}

func TestHasPresent(t *testing.T) {
	t.Parallel()

	key, val := []byte("key8"), []byte("value8")
	s := initStorageUnit(t, 10)
	err := s.Put(key, val)

	assert.Nil(t, err, "no error expected but got %s", err)

	err = s.Has(key)

	assert.Nil(t, err, "expected no error, but got %s", err)
}

func TestDeleteNotPresent(t *testing.T) {
	t.Parallel()

	key := []byte("key12")
	s := initStorageUnit(t, 10)
	err := s.Remove(key)

	assert.Nil(t, err, "expected no error, but got %s", err)
}

func TestDeleteNotPresentCache(t *testing.T) {
	t.Parallel()

	key, val := []byte("key13"), []byte("value13")
	s := initStorageUnit(t, 10)
	err := s.Put(key, val)
	assert.Nil(t, err, "Could not put value in storage unit")

	err = s.Has(key)

	assert.Nil(t, err, "expected no error, but got %s", err)

	s.ClearCache()

	err = s.Remove(key)
	assert.Nil(t, err, "expected no error, but got %s", err)

	err = s.Has(key)

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "key not found")
}

func TestDeletePresent(t *testing.T) {
	t.Parallel()

	key, val := []byte("key14"), []byte("value14")
	s := initStorageUnit(t, 10)
	err := s.Put(key, val)
	assert.Nil(t, err, "Could not put value in storage unit")

	err = s.Has(key)

	assert.Nil(t, err, "expected no error, but got %s", err)

	err = s.Remove(key)

	assert.Nil(t, err, "expected no error, but got %s", err)

	err = s.Has(key)

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "key not found")
}

func TestClearCacheNotAffectPersist(t *testing.T) {
	t.Parallel()

	key, val := []byte("key15"), []byte("value15")
	s := initStorageUnit(t, 10)
	err := s.Put(key, val)
	assert.Nil(t, err, "Could not put value in storage unit")
	s.ClearCache()

	err = s.Has(key)

	assert.Nil(t, err, "no error expected, but got %s", err)
}

func TestDestroyUnitNoError(t *testing.T) {
	t.Parallel()

	s := initStorageUnit(t, 10)
	err := s.DestroyUnit()
	assert.Nil(t, err, "no error expected, but got %s", err)
}

func TestCreateCacheFromConfWrongType(t *testing.T) {
	t.Parallel()

	cacher, err := storageUnit.NewCache(
		storageUnit.CacheCreationConfig{
			CacheConfig: storageUnit.CacheConfig{
				Type:        "NotLRU",
				Capacity:    100,
				Shards:      1,
				SizeInBytes: 0,
			},
		})

	assert.NotNil(t, err, "error expected")
	assert.Nil(t, cacher, "cacher expected to be nil, but got %s", cacher)
}

func TestCreateCacheFromConfOK(t *testing.T) {
	t.Parallel()

	cacher, err := storageUnit.NewCache(
		storageUnit.CacheCreationConfig{
			CacheConfig: storageUnit.CacheConfig{
				Type:        storageUnit.LRUCache,
				Capacity:    10,
				Shards:      1,
				SizeInBytes: 0,
			},
		})

	assert.Nil(t, err, "no error expected but got %s", err)
	assert.NotNil(t, cacher, "valid cacher expected but got nil")
}

func TestCreateDBFromConfWrongType(t *testing.T) {
	t.Parallel()

	arg := storageUnit.ArgDB{
		DBType:            "NotLvlDB",
		Path:              t.TempDir(),
		BatchDelaySeconds: 10,
		MaxBatchSize:      10,
		MaxOpenFiles:      10,
	}
	persister, err := storageUnit.NewDB(arg)

	assert.NotNil(t, err, "error expected")
	assert.Nil(t, persister, "persister expected to be nil, but got %s", persister)
}

func TestCreateDBFromConfWrongFileNameLvlDB(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("this is not a short test")
	}

	arg := storageUnit.ArgDB{
		DBType:            storageUnit.LvlDB,
		Path:              "",
		BatchDelaySeconds: 10,
		MaxBatchSize:      10,
		MaxOpenFiles:      10,
	}
	persister, err := storageUnit.NewDB(arg)
	assert.NotNil(t, err, "error expected")
	assert.Nil(t, persister, "persister expected to be nil, but got %s", persister)
}

func TestCreateDBFromConfLvlDBOk(t *testing.T) {
	t.Parallel()

	arg := storageUnit.ArgDB{
		DBType:            storageUnit.LvlDB,
		Path:              t.TempDir(),
		BatchDelaySeconds: 10,
		MaxBatchSize:      10,
		MaxOpenFiles:      10,
	}
	persister, err := storageUnit.NewDB(arg)
	assert.Nil(t, err, "no error expected")
	assert.NotNil(t, persister, "valid persister expected but got nil")

	err = persister.Destroy()
	assert.Nil(t, err, "no error expected destroying the persister")
}

func TestNewStorageUnit_FromConfWrongCacheSizeVsBatchSize(t *testing.T) {
	t.Parallel()

	storer, err := storageUnit.NewStorageUnitFromConf(
		storageUnit.CacheCreationConfig{
			CacheConfig: storageUnit.CacheConfig{
				Capacity: 10,
				Type:     storageUnit.LRUCache,
			},
		},
		storageUnit.DBConfig{
			FilePath:          t.TempDir(),
			Type:              storageUnit.LvlDB,
			MaxBatchSize:      11,
			BatchDelaySeconds: 1,
			MaxOpenFiles:      10,
		},
	)

	assert.NotNil(t, err, "error expected")
	assert.Nil(t, storer, "storer expected to be nil but got %s", storer)
}

func TestNewStorageUnit_FromConfWrongCacheConfig(t *testing.T) {
	t.Parallel()

	storer, err := storageUnit.NewStorageUnitFromConf(
		storageUnit.CacheCreationConfig{
			CacheConfig: storageUnit.CacheConfig{
				Capacity: 10,
				Type:     "NotLRU",
			},
		},
		storageUnit.DBConfig{
			FilePath:          t.TempDir(),
			Type:              storageUnit.LvlDB,
			BatchDelaySeconds: 1,
			MaxBatchSize:      1,
			MaxOpenFiles:      10,
		},
	)

	assert.NotNil(t, err, "error expected")
	assert.Nil(t, storer, "storer expected to be nil but got %s", storer)
}

func TestNewStorageUnit_FromConfWrongDBConfig(t *testing.T) {
	t.Parallel()

	storer, err := storageUnit.NewStorageUnitFromConf(
		storageUnit.CacheCreationConfig{
			CacheConfig: storageUnit.CacheConfig{
				Capacity: 10,
				Type:     storageUnit.LRUCache,
			},
		},
		storageUnit.DBConfig{
			FilePath: t.TempDir(),
			Type:     "NotLvlDB",
		},
	)

	assert.NotNil(t, err, "error expected")
	assert.Nil(t, storer, "storer expected to be nil but got %s", storer)
}

func TestNewStorageUnit_FromConfLvlDBOk(t *testing.T) {
	t.Parallel()

	storer, err := storageUnit.NewStorageUnitFromConf(
		storageUnit.CacheCreationConfig{
			CacheConfig: storageUnit.CacheConfig{
				Capacity: 10,
				Type:     storageUnit.LRUCache,
			},
		},
		storageUnit.DBConfig{
			FilePath:          t.TempDir(),
			Type:              storageUnit.LvlDB,
			MaxBatchSize:      1,
			BatchDelaySeconds: 1,
			MaxOpenFiles:      10,
		},
	)

	assert.Nil(t, err, "no error expected but got %s", err)
	assert.NotNil(t, storer, "valid storer expected but got nil")
	err = storer.DestroyUnit()
	assert.Nil(t, err, "no error expected destroying the persister")
}

func TestNewStorageUnit_ShouldWorkLvlDB(t *testing.T) {
	t.Parallel()

	storer, err := storageUnit.NewStorageUnitFromConf(
		storageUnit.CacheCreationConfig{
			CacheConfig: storageUnit.CacheConfig{
				Capacity: 10,
				Type:     storageUnit.LRUCache,
			},
		},
		storageUnit.DBConfig{
			FilePath:          t.TempDir(),
			Type:              storageUnit.LvlDB,
			BatchDelaySeconds: 1,
			MaxBatchSize:      1,
			MaxOpenFiles:      10,
		},
	)

	assert.Nil(t, err, "no error expected but got %s", err)
	assert.NotNil(t, storer, "valid storer expected but got nil")
	err = storer.DestroyUnit()
	assert.Nil(t, err, "no error expected destroying the persister")
}

func TestNewStorageUnit_WrongConfigForRemovalCache(t *testing.T) {
	t.Parallel()

	storer, err := storageUnit.NewStorageUnitFromConf(
		storageUnit.CacheCreationConfig{
			CacheConfig: storageUnit.CacheConfig{
				Capacity: 10,
				Type:     storageUnit.LRUCache,
			},
			RemovalTrackingCacheConfig: storageUnit.CacheConfig{
				Capacity: 10,
				Type:     "",
			},
		},
		storageUnit.DBConfig{
			FilePath:          t.TempDir(),
			Type:              storageUnit.LvlDB,
			BatchDelaySeconds: 1,
			MaxBatchSize:      1,
			MaxOpenFiles:      10,
		},
	)

	assert.ErrorIs(t, err, common.ErrNotSupportedCacheType)
	assert.Contains(t, err.Error(), "when creating removal cache")
	assert.Nil(t, storer)
}

func TestNewStorageUnit_ShouldWorkWithRemovalCache(t *testing.T) {
	t.Parallel()

	storer, err := storageUnit.NewStorageUnitFromConf(
		storageUnit.CacheCreationConfig{
			CacheConfig: storageUnit.CacheConfig{
				Capacity: 10,
				Type:     storageUnit.LRUCache,
			},
			RemovalTrackingCacheConfig: storageUnit.CacheConfig{
				Capacity: 10,
				Type:     storageUnit.LRUCache,
			},
		},
		storageUnit.DBConfig{
			FilePath:          t.TempDir(),
			Type:              storageUnit.LvlDB,
			BatchDelaySeconds: 1,
			MaxBatchSize:      1,
			MaxOpenFiles:      10,
		},
	)

	assert.Nil(t, err)
	assert.NotNil(t, storer)

	_ = storer.DestroyUnit()
}

const (
	valuesInDb = 100000
)

func BenchmarkStorageUnit_Put(b *testing.B) {
	b.StopTimer()
	s := initStorageUnit(b, 1)
	defer func() {
		err := s.DestroyUnit()
		logError(err)
	}()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nr := rand.Intn(valuesInDb)
		b.StartTimer()

		err := s.Put([]byte(strconv.Itoa(nr)), []byte(strconv.Itoa(nr)))
		logError(err)
	}
}

func BenchmarkStorageUnit_GetWithDataBeingPresent(b *testing.B) {
	b.StopTimer()
	s := initStorageUnit(b, 1)
	defer func() {
		err := s.DestroyUnit()
		logError(err)
	}()
	for i := 0; i < valuesInDb; i++ {
		err := s.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
		logError(err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nr := rand.Intn(valuesInDb)
		b.StartTimer()

		_, err := s.Get([]byte(strconv.Itoa(nr)))
		logError(err)
	}
}

func BenchmarkStorageUnit_GetWithDataNotBeingPresent(b *testing.B) {
	b.StopTimer()
	s := initStorageUnit(b, 1)
	defer func() {
		err := s.DestroyUnit()
		logError(err)
	}()
	for i := 0; i < valuesInDb; i++ {
		err := s.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
		logError(err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nr := rand.Intn(valuesInDb) + valuesInDb
		b.StartTimer()

		_, err := s.Get([]byte(strconv.Itoa(nr)))
		logError(err)
	}
}
