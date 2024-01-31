package storageUnit

import (
	"encoding/base64"
	"fmt"
	"sync"

	"github.com/multiversx/mx-chain-core-go/core/check"
	"github.com/multiversx/mx-chain-core-go/data"
	logger "github.com/multiversx/mx-chain-logger-go"
	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/factory"
	"github.com/multiversx/mx-chain-storage-go/types"
)

var _ types.Storer = (*Unit)(nil)

var log = logger.GetOrCreate("storage/storageUnit")

// PersisterFactoryHandler defines the behaviour of a component which is able to create persisters
type PersisterFactoryHandler interface {
	Create(path string) (types.Persister, error)
	CreateWithRetries(path string) (types.Persister, error)
	IsInterfaceNil() bool
}

// Unit represents a storer's data bank
// holding the cache and persistence unit
type Unit struct {
	lock      sync.RWMutex
	persister types.Persister
	cacher    types.Cacher
}

// NewStorageUnit is the constructor for the storage unit, creating a new storage unit
// from the given cacher and persister.
func NewStorageUnit(c types.Cacher, p types.Persister) (*Unit, error) {
	if check.IfNil(p) {
		return nil, common.ErrNilPersister
	}
	if check.IfNil(c) {
		return nil, common.ErrNilCacher
	}

	sUnit := &Unit{
		persister: p,
		cacher:    c,
	}

	return sUnit, nil
}

// NewStorageUnitFromConf creates a new storage unit from a storage unit config
func NewStorageUnitFromConf(cacheConf common.CacheConfig, dbConf common.DBConfig, persisterFactory PersisterFactoryHandler) (*Unit, error) {
	var cache types.Cacher
	var db types.Persister
	var err error

	// TODO: if there will be a differentiation between the creation or opening of a DB, the DB could be destroyed
	// in case of a failure while creating (not opening).

	if dbConf.MaxBatchSize > int(cacheConf.Capacity) {
		return nil, common.ErrCacheSizeIsLowerThanBatchSize
	}

	cache, err = factory.NewCache(cacheConf)
	if err != nil {
		return nil, err
	}

	db, err = persisterFactory.CreateWithRetries(dbConf.FilePath)
	if err != nil {
		return nil, err
	}

	return NewStorageUnit(cache, db)
}

// Put adds data to both cache and persistence medium
func (u *Unit) Put(key, data []byte) error {
	u.lock.Lock()
	defer u.lock.Unlock()

	u.cacher.Put(key, data, len(data))

	err := u.persister.Put(key, data)
	if err != nil {
		u.cacher.Remove(key)
		return err
	}

	return err
}

// PutInEpoch will call the Put method as this storer doesn't handle epochs
func (u *Unit) PutInEpoch(key, data []byte, _ uint32) error {
	return u.Put(key, data)
}

// GetOldestEpoch will return an error that signals that the oldest epoch fetching is not available
func (u *Unit) GetOldestEpoch() (uint32, error) {
	return 0, common.ErrOldestEpochNotAvailable
}

// Close will close unit
func (u *Unit) Close() error {
	u.cacher.Clear()

	err := u.persister.Close()
	if err != nil {
		log.Error("cannot close storage unit persister", "error", err)
		return err
	}

	return nil
}

// RangeKeys can iterate over the persisted (key, value) pairs calling the provided handler
func (u *Unit) RangeKeys(handler func(key []byte, value []byte) bool) {
	u.persister.RangeKeys(handler)
}

// Get searches the key in the cache. In case it is not found,
// it further searches it in the associated database.
// In case it is found in the database, the cache is updated with the value as well.
func (u *Unit) Get(key []byte) ([]byte, error) {
	u.lock.Lock()
	defer u.lock.Unlock()

	v, ok := u.cacher.Get(key)
	var err error

	if !ok {
		// not found in cache
		// search it in second persistence medium

		v, err = u.persister.Get(key)
		if err != nil {
			return nil, err
		}

		buff, okAssertion := v.([]byte)
		if !okAssertion {
			return nil, fmt.Errorf("key: %s is not a byte slice", base64.StdEncoding.EncodeToString(key))
		}

		// if found in persistence unit, add it in cache
		u.cacher.Put(key, v, len(buff))
	}

	return v.([]byte), nil
}

// GetFromEpoch will call the Get method as this storer doesn't handle epochs
func (u *Unit) GetFromEpoch(key []byte, _ uint32) ([]byte, error) {
	return u.Get(key)
}

// GetBulkFromEpoch will call the Get method for all keys as this storer doesn't handle epochs
func (u *Unit) GetBulkFromEpoch(keys [][]byte, _ uint32) ([]data.KeyValuePair, error) {
	results := make([]data.KeyValuePair, 0, len(keys))
	for _, key := range keys {
		value, err := u.Get(key)
		if err != nil {
			log.Warn("cannot get key from unit",
				"key", key,
				"error", err.Error(),
			)
			continue
		}
		keyValue := data.KeyValuePair{Key: key, Value: value}
		results = append(results, keyValue)
	}
	return results, nil
}

// Has checks if the key is in the Unit.
// It first checks the cache. If it is not found, it checks the db
func (u *Unit) Has(key []byte) error {
	u.lock.RLock()
	defer u.lock.RUnlock()

	has := u.cacher.Has(key)
	if has {
		return nil
	}

	return u.persister.Has(key)
}

// SearchFirst will call the Get method as this storer doesn't handle epochs
func (u *Unit) SearchFirst(key []byte) ([]byte, error) {
	return u.Get(key)
}

// RemoveFromCurrentEpoch removes the data associated to the given key from both cache and persistence medium
func (u *Unit) RemoveFromCurrentEpoch(key []byte) error {
	return u.Remove(key)
}

// Remove removes the data associated to the given key from both cache and persistence medium
func (u *Unit) Remove(key []byte) error {
	u.lock.Lock()
	defer u.lock.Unlock()

	u.cacher.Remove(key)
	err := u.persister.Remove(key)

	return err
}

// ClearCache cleans up the entire cache
func (u *Unit) ClearCache() {
	u.cacher.Clear()
}

// DestroyUnit cleans up the cache, and the db
func (u *Unit) DestroyUnit() error {
	u.lock.Lock()
	defer u.lock.Unlock()

	u.cacher.Clear()
	return u.persister.Destroy()
}

// IsInterfaceNil returns true if there is no value under the interface
func (u *Unit) IsInterfaceNil() bool {
	return u == nil
}
