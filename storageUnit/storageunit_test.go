package storageUnit_test

import (
	"testing"

	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/lrucache"
	"github.com/multiversx/mx-chain-storage-go/memorydb"
	"github.com/multiversx/mx-chain-storage-go/storageUnit"
	"github.com/stretchr/testify/assert"
)

func initStorageUnit(tb testing.TB, cSize int) *storageUnit.Unit {
	mdb := memorydb.New()
	cache, err2 := lrucache.NewCache(cSize)
	assert.Nil(tb, err2, "no error expected but got %s", err2)

	sUnit, err := storageUnit.NewStorageUnit(cache, mdb)
	assert.Nil(tb, err, "failed to create storage unit")

	return sUnit
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
