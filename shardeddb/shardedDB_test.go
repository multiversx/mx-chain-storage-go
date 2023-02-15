package shardeddb_test

import (
	"testing"

	"github.com/multiversx/mx-chain-storage-go/shardeddb"
	"github.com/multiversx/mx-chain-storage-go/storageUnit"
	"github.com/multiversx/mx-chain-storage-go/testscommon"
	"github.com/stretchr/testify/require"
)

func TestNewShardedPersister(t *testing.T) {
	t.Parallel()

	t.Run("nil id provider", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		db, err := shardeddb.NewShardedPersister(storageUnit.LvlDBSerial, dir, 2, 10, 10, nil)
		require.Nil(t, db)
		require.Equal(t, shardeddb.ErrNilIDProvider, err)
	})

	t.Run("should work", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		db, err := shardeddb.NewShardedPersister(storageUnit.LvlDBSerial, dir, 2, 10, 10, &testscommon.ShardIDProviderStub{})
		require.NotNil(t, db)
		require.Nil(t, err)
	})
}

func TestShardedPersister_Operations(t *testing.T) {
	t.Parallel()

	idProvider, err := shardeddb.NewShardIDProvider(4)
	require.Nil(t, err)

	dir := t.TempDir()
	db, err := shardeddb.NewShardedPersister(storageUnit.LvlDBSerial, dir, 2, 10, 10, idProvider)
	require.Nil(t, err)

	_ = db.Put([]byte("aaa"), []byte("aaaval"))
	_ = db.Put([]byte("aab"), []byte("aabval"))
	_ = db.Put([]byte("aac"), []byte("aacval"))

	err = db.Close()
	require.Nil(t, err)

	db2, err := shardeddb.NewShardedPersister(storageUnit.LvlDBSerial, dir, 2, 10, 10, idProvider)
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
