package pebbledb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/types"
)

var _ types.Persister = (*DB)(nil)

const rwxOwner = 0700

// walSyncInterval bounds in time the writes a power loss can take, independent of the write rate
const walSyncInterval = 5 * time.Second

// ArgsNewDB holds the arguments needed to create a pebble persister
type ArgsNewDB struct {
	Path                  string
	BatchDelaySeconds     int
	MaxBatchSize          int
	MaxOpenFiles          int
	BloomFilterBitsPerKey int
	Profile               string
	SharedResources       *SharedResources
}

// DB is a pebble backed persister that buffers writes and commits them in batches without fsync;
// the WAL is synced in the background and on close
type DB struct {
	path   string
	db     *pebble.DB
	access *accessGuard

	mutBatch     sync.RWMutex
	batch        *pendingBatch
	flushing     *pendingBatch
	sizeBatch    int
	maxBatchSize int
	mutCommit    sync.Mutex

	dirtySinceSync atomic.Bool
	cancel         context.CancelFunc
	mutClose       sync.Mutex
}

// NewDB creates the directory if needed and opens (or creates) the pebble database in it
func NewDB(args ArgsNewDB) (*DB, error) {
	if args.MaxOpenFiles < 1 {
		return nil, common.ErrInvalidNumOpenFiles
	}
	if args.BatchDelaySeconds < 1 || args.MaxBatchSize < 1 {
		return nil, fmt.Errorf("%w: batch delay and max batch size must be positive", common.ErrInvalidConfig)
	}

	opts, err := buildOptions(args)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(args.Path, rwxOwner)
	if err != nil {
		return nil, err
	}

	db, err := pebble.Open(args.Path, opts)
	if err != nil {
		return nil, fmt.Errorf("%w for path %s", err, args.Path)
	}

	ctx, cancel := context.WithCancel(context.Background())
	dbStore := &DB{
		path:         args.Path,
		db:           db,
		access:       newAccessGuard(),
		batch:        newPendingBatch(),
		maxBatchSize: args.MaxBatchSize,
		cancel:       cancel,
	}

	go dbStore.backgroundLoop(ctx, time.Duration(args.BatchDelaySeconds)*time.Second)

	runtime.SetFinalizer(dbStore, func(db *DB) {
		_ = db.Close()
	})

	log.Debug("opened pebble db persister", "path", args.Path, "profile", args.Profile)

	return dbStore, nil
}

func (s *DB) backgroundLoop(ctx context.Context, batchDelay time.Duration) {
	batchTicker := time.NewTicker(batchDelay)
	defer batchTicker.Stop()
	syncTicker := time.NewTicker(walSyncInterval)
	defer syncTicker.Stop()

	for {
		select {
		case <-batchTicker.C:
			err := s.commitPending()
			if err != nil && !errors.Is(err, common.ErrDBIsClosed) {
				log.Warn("pebbledb commit", "path", s.path, "error", err.Error())
			}
		case <-syncTicker.C:
			s.syncWAL()
		case <-ctx.Done():
			return
		}
	}
}

// Put adds the value to the (key, val) storage medium
func (s *DB) Put(key, val []byte) error {
	return s.buffer(func(batch *pendingBatch) {
		batch.put(key, val)
	})
}

// Remove removes the data associated to the given key
func (s *DB) Remove(key []byte) error {
	return s.buffer(func(batch *pendingBatch) {
		batch.remove(key)
	})
}

// buffer applies the operation to the pending batch and commits it once it holds maxBatchSize operations
func (s *DB) buffer(operation func(batch *pendingBatch)) error {
	if s.access.isClosed() {
		return common.ErrDBIsClosed
	}

	s.mutBatch.Lock()
	operation(s.batch)
	s.sizeBatch++
	if s.sizeBatch < s.maxBatchSize {
		s.mutBatch.Unlock()
		return nil
	}
	toCommit := s.swapBatch()
	s.mutBatch.Unlock()

	return s.commit(toCommit)
}

func (s *DB) commitPending() error {
	s.mutBatch.Lock()
	if s.batch.isEmpty() {
		s.mutBatch.Unlock()
		return nil
	}
	toCommit := s.swapBatch()
	s.mutBatch.Unlock()

	return s.commit(toCommit)
}

// swapBatch must be called under mutBatch lock
func (s *DB) swapBatch() *pendingBatch {
	toCommit := s.batch
	s.batch = newPendingBatch()
	s.sizeBatch = 0

	return toCommit
}

// commit keeps the batch readable while it is being applied, so reads never miss a buffered write
func (s *DB) commit(toCommit *pendingBatch) error {
	s.mutCommit.Lock()
	defer s.mutCommit.Unlock()

	s.setFlushing(toCommit)
	defer s.setFlushing(nil)

	if !s.access.acquire() {
		return common.ErrDBIsClosed
	}
	defer s.access.release()

	err := toCommit.applyTo(s.db)
	if err == nil {
		s.dirtySinceSync.Store(true)
	}

	return err
}

func (s *DB) setFlushing(batch *pendingBatch) {
	s.mutBatch.Lock()
	s.flushing = batch
	s.mutBatch.Unlock()
}

// Get returns the value associated to the key
func (s *DB) Get(key []byte) ([]byte, error) {
	if s.access.isClosed() {
		return nil, common.ErrDBIsClosed
	}

	val, removed, found := s.lookupBuffered(key)
	if removed {
		return nil, common.ErrKeyNotFound
	}
	if found {
		return cloneBytes(val), nil
	}

	return s.readFromEngine(key, true)
}

// Has returns nil if the given key is present in the persistence medium
func (s *DB) Has(key []byte) error {
	if s.access.isClosed() {
		return common.ErrDBIsClosed
	}

	_, removed, found := s.lookupBuffered(key)
	if removed {
		return common.ErrKeyNotFound
	}
	if found {
		return nil
	}

	_, err := s.readFromEngine(key, false)

	return err
}

// readFromEngine copies the value only when asked, since the engine slice is valid until the closer is called
func (s *DB) readFromEngine(key []byte, wantValue bool) ([]byte, error) {
	if !s.access.acquire() {
		return nil, common.ErrDBIsClosed
	}
	defer s.access.release()

	value, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, common.ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}

	var result []byte
	if wantValue {
		result = cloneBytes(value)
	}
	_ = closer.Close()

	return result, nil
}

func (s *DB) lookupBuffered(key []byte) (val []byte, removed bool, found bool) {
	s.mutBatch.RLock()
	defer s.mutBatch.RUnlock()

	val, removed, found = s.batch.lookup(key)
	if removed || found || s.flushing == nil {
		return val, removed, found
	}

	return s.flushing.lookup(key)
}

// RangeKeys will call the handler function for each committed (key, value) pair
// If the handler returns true, the iteration will continue, otherwise will stop
func (s *DB) RangeKeys(handler func(key []byte, value []byte) bool) {
	if handler == nil {
		return
	}

	if !s.access.acquire() {
		return
	}
	defer s.access.release()

	iterator, err := s.db.NewIter(nil)
	if err != nil {
		log.Warn("pebbledb.RangeKeys", "path", s.path, "error", err.Error())
		return
	}
	defer func() {
		_ = iterator.Close()
	}()

	for valid := iterator.First(); valid; valid = iterator.Next() {
		if !handler(cloneBytes(iterator.Key()), cloneBytes(iterator.Value())) {
			break
		}
	}
}

func cloneBytes(data []byte) []byte {
	cloned := make([]byte, len(data))
	copy(cloned, data)

	return cloned
}

func (s *DB) syncWAL() {
	if !s.dirtySinceSync.Swap(false) {
		return
	}

	if !s.access.acquire() {
		return
	}
	defer s.access.release()

	err := s.db.LogData(nil, pebble.Sync)
	if err != nil {
		log.Warn("pebbledb WAL sync", "path", s.path, "error", err.Error())
		s.dirtySinceSync.Store(true)
	}
}

// Close commits the buffered writes, syncs the WAL and closes the database
func (s *DB) Close() error {
	s.mutClose.Lock()
	defer s.mutClose.Unlock()

	_ = s.commitPending()

	if !s.access.closeAndWait() {
		return nil
	}
	s.cancel()

	err := s.db.LogData(nil, pebble.Sync)
	if err != nil {
		log.Warn("pebbledb WAL sync on close", "path", s.path, "error", err.Error())
	}

	log.Debug("closed pebble db persister", "path", s.path)

	return s.db.Close()
}

// Destroy removes the storage medium stored data
func (s *DB) Destroy() error {
	log.Debug("pebbledb.Destroy", "path", s.path)

	err := s.Close()
	if err != nil {
		return err
	}

	return os.RemoveAll(s.path)
}

// DestroyClosed removes the already closed storage medium stored data
func (s *DB) DestroyClosed() error {
	err := os.RemoveAll(s.path)
	if err != nil {
		log.Error("error destroy closed", "error", err, "path", s.path)
	}

	return err
}

// IsInterfaceNil returns true if there is no value under the interface
func (s *DB) IsInterfaceNil() bool {
	return s == nil
}
