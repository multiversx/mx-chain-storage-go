package factory_test

import (
	"fmt"
	"testing"

	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/factory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDB(t *testing.T) {
	t.Parallel()

	t.Run("wrong db type, should fail", func(t *testing.T) {
		t.Parallel()

		argsDB := factory.ArgDB{
			DBType:            "NotLvlDB",
			Path:              "test",
			BatchDelaySeconds: 10,
			MaxBatchSize:      10,
			MaxOpenFiles:      10,
		}
		persister, err := factory.NewDB(argsDB)

		require.Equal(t, common.ErrNotSupportedDBType, err)
		require.Nil(t, persister)
	})

	t.Run("wrong file path, should fail", func(t *testing.T) {
		t.Parallel()

		path := ""
		argsDB := factory.ArgDB{
			DBType:            common.LvlDB,
			Path:              path,
			BatchDelaySeconds: 10,
			MaxBatchSize:      10,
			MaxOpenFiles:      10,
		}
		persister, err := factory.NewDB(argsDB)
		assert.NotNil(t, err)
		assert.Nil(t, persister)
	})

	t.Run("LvlDB type, should work", func(t *testing.T) {
		t.Parallel()

		path := t.TempDir()

		argsDB := factory.ArgDB{
			DBType:            common.LvlDB,
			Path:              path,
			BatchDelaySeconds: 10,
			MaxBatchSize:      10,
			MaxOpenFiles:      10,
		}
		persister, err := factory.NewDB(argsDB)
		require.Nil(t, err)
		require.Equal(t, "*leveldb.DB", fmt.Sprintf("%T", persister))

		err = persister.Close()
		require.Nil(t, err)
	})

	t.Run("LvlDBSerial type, should work", func(t *testing.T) {
		t.Parallel()

		path := t.TempDir()

		argsDB := factory.ArgDB{
			DBType:            common.LvlDBSerial,
			Path:              path,
			BatchDelaySeconds: 10,
			MaxBatchSize:      10,
			MaxOpenFiles:      10,
		}
		persister, err := factory.NewDB(argsDB)
		require.Nil(t, err)
		require.Equal(t, "*leveldb.SerialDB", fmt.Sprintf("%T", persister))

		err = persister.Close()
		require.Nil(t, err)
	})

	t.Run("MemoryDB type, should work", func(t *testing.T) {
		t.Parallel()

		path := t.TempDir()

		argsDB := factory.ArgDB{
			DBType:            common.MemoryDB,
			Path:              path,
			BatchDelaySeconds: 10,
			MaxBatchSize:      10,
			MaxOpenFiles:      10,
		}
		persister, err := factory.NewDB(argsDB)
		require.Nil(t, err)
		require.Equal(t, "*memorydb.DB", fmt.Sprintf("%T", persister))

		err = persister.Close()
		require.Nil(t, err)
	})
}
