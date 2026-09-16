package pebbledb

import (
	"github.com/cockroachdb/pebble/v2"
)

// pendingBatch buffers writes until they are committed; the last operation on a key wins, so the
// two maps never hold the same key. Values are copied in, so callers may reuse their buffers.
type pendingBatch struct {
	puts    map[string][]byte
	deletes map[string]struct{}
}

func newPendingBatch() *pendingBatch {
	return &pendingBatch{
		puts:    make(map[string][]byte),
		deletes: make(map[string]struct{}),
	}
}

func (b *pendingBatch) put(key []byte, val []byte) {
	b.puts[string(key)] = cloneBytes(val)
	delete(b.deletes, string(key))
}

func (b *pendingBatch) remove(key []byte) {
	b.deletes[string(key)] = struct{}{}
	delete(b.puts, string(key))
}

// lookup reports the buffered state of a key: (value, found) or removed
func (b *pendingBatch) lookup(key []byte) (val []byte, removed bool, found bool) {
	if _, removed = b.deletes[string(key)]; removed {
		return nil, true, false
	}

	val, found = b.puts[string(key)]

	return val, false, found
}

func (b *pendingBatch) isEmpty() bool {
	return len(b.puts) == 0 && len(b.deletes) == 0
}

func (b *pendingBatch) applyTo(db *pebble.DB) error {
	if b.isEmpty() {
		return nil
	}

	batch := db.NewBatch()
	for key, val := range b.puts {
		_ = batch.Set([]byte(key), val, nil)
	}
	for key := range b.deletes {
		_ = batch.Delete([]byte(key), nil)
	}

	err := db.Apply(batch, pebble.NoSync)
	_ = batch.Close()

	return err
}
