package leveldb_test

import (
	"testing"

	"github.com/multiversx/mx-chain-storage-go/leveldb"
	"github.com/stretchr/testify/require"
)

func TestNewShardIDProvider(t *testing.T) {
	t.Parallel()

	t.Run("invalid number of shards", func(t *testing.T) {
		t.Parallel()

		ip, err := leveldb.NewShardIDProvider(0)
		require.Nil(t, ip)
		require.Equal(t, leveldb.ErrInvalidNumberOfShards, err)
	})

	t.Run("should work", func(t *testing.T) {
		t.Parallel()

		ip, err := leveldb.NewShardIDProvider(4)
		require.Nil(t, err)
		require.NotNil(t, ip)
	})
}

func TestNumberOfShards(t *testing.T) {
	t.Parallel()

	numShards := uint32(4)

	ip, err := leveldb.NewShardIDProvider(numShards)
	require.Nil(t, err)

	require.Equal(t, numShards, ip.NumberOfShards())
}

func TestGetShardIDs(t *testing.T) {
	t.Parallel()

	ip, err := leveldb.NewShardIDProvider(uint32(4))
	require.Nil(t, err)

	expShardIDs := []uint32{0, 1, 2, 3}
	require.Equal(t, expShardIDs, ip.GetShardIDs())
}

func TestComputeId(t *testing.T) {
	t.Parallel()

	ip, err := leveldb.NewShardIDProvider(uint32(4))
	require.Nil(t, err)

	require.Equal(t, uint32(0), ip.ComputeId([]byte{0}))
	require.Equal(t, uint32(1), ip.ComputeId([]byte{1}))
	require.Equal(t, uint32(2), ip.ComputeId([]byte{2}))
	require.Equal(t, uint32(3), ip.ComputeId([]byte{3}))
}
