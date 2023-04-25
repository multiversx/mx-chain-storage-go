package testscommon

import "github.com/multiversx/mx-chain-storage-go/types"

// PersisterCreatorStub -
type PersisterCreatorStub struct {
	CreateBasePersisterCalled func(path string) (types.Persister, error)
	GetBasePathCalled         func() string
}

// CreateBasePersister -
func (stub *PersisterCreatorStub) CreateBasePersister(path string) (types.Persister, error) {
	if stub.CreateBasePersisterCalled != nil {
		return stub.CreateBasePersisterCalled(path)
	}

	return nil, nil
}

// GetBasePath -
func (stub *PersisterCreatorStub) GetBasePath() string {
	if stub.GetBasePathCalled != nil {
		return stub.GetBasePathCalled()
	}

	return ""
}

// IsInterfaceNil -
func (stub *PersisterCreatorStub) IsInterfaceNil() bool {
	return stub == nil
}
