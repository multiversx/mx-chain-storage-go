package sharded_test

import (
	"testing"

	"github.com/multiversx/mx-chain-storage-go/sharded"
	"github.com/stretchr/testify/require"
)

func TestNewShardIDProvider(t *testing.T) {
	t.Parallel()

	t.Run("invalid number of shards, zero", func(t *testing.T) {
		t.Parallel()

		ip, err := sharded.NewShardIDProvider(0)
		require.Nil(t, ip)
		require.Equal(t, sharded.ErrInvalidNumberOfShards, err)
	})

	t.Run("invalid number of shards, negative number", func(t *testing.T) {
		t.Parallel()

		ip, err := sharded.NewShardIDProvider(-1)
		require.Nil(t, ip)
		require.Equal(t, sharded.ErrInvalidNumberOfShards, err)
	})

	t.Run("should work", func(t *testing.T) {
		t.Parallel()

		ip, err := sharded.NewShardIDProvider(4)
		require.Nil(t, err)
		require.NotNil(t, ip)
	})
}

func TestNumberOfShards(t *testing.T) {
	t.Parallel()

	numShards := int32(4)

	ip, err := sharded.NewShardIDProvider(numShards)
	require.Nil(t, err)

	require.Equal(t, uint32(numShards), ip.NumberOfShards())
}

func TestGetShardIDs(t *testing.T) {
	t.Parallel()

	ip, err := sharded.NewShardIDProvider(int32(4))
	require.Nil(t, err)

	expShardIDs := []uint32{0, 1, 2, 3}
	require.Equal(t, expShardIDs, ip.GetShardIDs())
}

func TestComputeId(t *testing.T) {
	t.Parallel()

	t.Run("4 shards", func(t *testing.T) {
		t.Parallel()

		ip, err := sharded.NewShardIDProvider(int32(4))
		require.Nil(t, err)

		require.Equal(t, uint32(0), ip.ComputeId([]byte{0}))
		require.Equal(t, uint32(1), ip.ComputeId([]byte{1}))
		require.Equal(t, uint32(2), ip.ComputeId([]byte{2}))
		require.Equal(t, uint32(3), ip.ComputeId([]byte{3}))
	})

	t.Run("5 shards", func(t *testing.T) {
		t.Parallel()

		ip, err := sharded.NewShardIDProvider(int32(5))
		require.Nil(t, err)

		require.Equal(t, uint32(0), ip.ComputeId([]byte{0}))
		require.Equal(t, uint32(1), ip.ComputeId([]byte{1}))
		require.Equal(t, uint32(2), ip.ComputeId([]byte{2}))
		require.Equal(t, uint32(3), ip.ComputeId([]byte{3}))
		require.Equal(t, uint32(4), ip.ComputeId([]byte{4}))
		require.Equal(t, uint32(1), ip.ComputeId([]byte{5}))
		require.Equal(t, uint32(2), ip.ComputeId([]byte{6}))
		require.Equal(t, uint32(3), ip.ComputeId([]byte{7}))
		require.Equal(t, uint32(0), ip.ComputeId([]byte{8}))
		require.Equal(t, uint32(1), ip.ComputeId([]byte{9}))
	})
}
