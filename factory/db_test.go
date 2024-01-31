package factory_test

import (
	"testing"

	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/factory"
	"github.com/stretchr/testify/assert"
)

func TestCreateDBFromConfWrongType(t *testing.T) {
	t.Parallel()

	argsDB := factory.ArgDB{
		DBType:            "NotLvlDB",
		Path:              "test",
		BatchDelaySeconds: 10,
		MaxBatchSize:      10,
		MaxOpenFiles:      10,
	}
	persister, err := factory.NewDB(argsDB)

	assert.NotNil(t, err, "error expected")
	assert.Nil(t, persister, "persister expected to be nil, but got %s", persister)
}

func TestCreateDBFromConfWrongFileNameLvlDB(t *testing.T) {
	if testing.Short() {
		t.Skip("this is not a short test")
	}

	path := ""
	argsDB := factory.ArgDB{
		DBType:            common.LvlDB,
		Path:              path,
		BatchDelaySeconds: 10,
		MaxBatchSize:      10,
		MaxOpenFiles:      10,
	}
	persister, err := factory.NewDB(argsDB)
	assert.NotNil(t, err, "error expected")
	assert.Nil(t, persister, "persister expected to be nil, but got %s", persister)
}

func TestCreateDBFromConfLvlDBOk(t *testing.T) {
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
	assert.Nil(t, err, "no error expected")
	assert.NotNil(t, persister, "valid persister expected but got nil")

	err = persister.Destroy()
	assert.Nil(t, err, "no error expected destroying the persister")
}
