package leveldb

import (
	"errors"
	"fmt"

	"github.com/multiversx/mx-chain-core-go/core/check"
	"github.com/multiversx/mx-chain-storage-go/types"
)

var _ types.Persister = (*shardedPersister)(nil)

// ErrNilIDProvider signals that a nil id provider was provided
var ErrNilIDProvider = errors.New("nil id provider")

type shardedPersister struct {
	persisters map[uint32]*SerialDB
	idProvider types.ShardIDProvider
}

// NewShardedPersister will created a new sharded persister
func NewShardedPersister(path string, batchDelaySeconds int, maxBatchSize int, maxOpenFilesPerShard int, idProvider types.ShardIDProvider) (*shardedPersister, error) {
	if check.IfNil(idProvider) {
		return nil, ErrNilIDProvider
	}

	persisters := make(map[uint32]*SerialDB)
	for _, shardID := range idProvider.GetShardIDs() {
		newPath := updatePathWithShardID(path, shardID)
		db, err := NewSerialDB(newPath, batchDelaySeconds, maxBatchSize, maxOpenFilesPerShard)
		if err != nil {
			return nil, err
		}
		persisters[shardID] = db
	}

	return &shardedPersister{
		persisters: persisters,
		idProvider: idProvider,
	}, nil
}

func updatePathWithShardID(path string, shardID uint32) string {
	return fmt.Sprintf("%s_%d", path, shardID)
}

func (s *shardedPersister) computeID(key []byte) uint32 {
	return s.idProvider.ComputeId(key)
}

// Put adds the value at the associated key in the persistence medium
func (s *shardedPersister) Put(key []byte, val []byte) error {
	return s.persisters[s.computeID(key)].Put(key, val)
}

// Get gets the value associated to the key
func (s *shardedPersister) Get(key []byte) ([]byte, error) {
	return s.persisters[s.computeID(key)].Get(key)
}

// Has returns true if the given key is present in the persistence medium
func (s *shardedPersister) Has(key []byte) error {
	return s.persisters[s.computeID(key)].Has(key)
}

// Close closes the files/resources associated to the persistence medium
func (s *shardedPersister) Close() error {
	for _, persister := range s.persisters {
		err := persister.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

// Remove removes the data associated to the given key
func (s *shardedPersister) Remove(key []byte) error {
	return s.persisters[s.computeID(key)].Remove(key)
}

// Destroy removes the persistence medium stored data
func (s *shardedPersister) Destroy() error {
	for _, persister := range s.persisters {
		err := persister.Destroy()
		if err != nil {
			return err
		}
	}

	return nil
}

// DestroyClosed removes the already closed persistence medium stored data
func (s *shardedPersister) DestroyClosed() error {
	for _, persister := range s.persisters {
		err := persister.DestroyClosed()
		if err != nil {
			return err
		}
	}

	return nil
}

// RangeKeys will iterate over all contained pairs, in all persisters, calling te provided handler
func (s *shardedPersister) RangeKeys(handler func(key []byte, val []byte) bool) {
	for _, persister := range s.persisters {
		persister.RangeKeys(handler)
	}
}

// IsInterfaceNil returns true if there is no value under the interface
func (s *shardedPersister) IsInterfaceNil() bool {
	return s == nil
}
