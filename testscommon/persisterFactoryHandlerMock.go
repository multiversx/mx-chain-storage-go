package testscommon

import (
	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/leveldb"
	"github.com/multiversx/mx-chain-storage-go/memorydb"
	"github.com/multiversx/mx-chain-storage-go/types"
)

type persisterFactoryHandlerMock struct {
	dbType            common.DBType
	batchDelaySeconds int
	maxBatchSize      int
	maxOpenFiles      int
}

// NewPersisterFactoryHandlerMock -
func NewPersisterFactoryHandlerMock(dbType common.DBType, batchDelaySeconds int, maxBatchSize int, maxOpenFiles int) *persisterFactoryHandlerMock {
	return &persisterFactoryHandlerMock{
		dbType:            dbType,
		batchDelaySeconds: batchDelaySeconds,
		maxBatchSize:      maxBatchSize,
		maxOpenFiles:      maxOpenFiles,
	}
}

// CreateWithRetries -
func (mock *persisterFactoryHandlerMock) CreateWithRetries(path string) (types.Persister, error) {
	return mock.Create(path)
}

// Create -
func (mock *persisterFactoryHandlerMock) Create(path string) (types.Persister, error) {
	switch mock.dbType {
	case common.LvlDB:
		return leveldb.NewDB(path, mock.batchDelaySeconds, mock.maxBatchSize, mock.maxOpenFiles)
	case common.LvlDBSerial:
		return leveldb.NewSerialDB(path, mock.batchDelaySeconds, mock.maxBatchSize, mock.maxOpenFiles)
	case common.MemoryDB:
		return memorydb.New(), nil
	default:
		return nil, common.ErrNotSupportedDBType
	}
}

// IsInterfaceNil -
func (mock *persisterFactoryHandlerMock) IsInterfaceNil() bool {
	return mock == nil
}
