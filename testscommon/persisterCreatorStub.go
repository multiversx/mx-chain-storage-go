package testscommon

import "github.com/multiversx/mx-chain-storage-go/types"

// PersisterCreatorStub -
type PersisterCreatorStub struct {
	CreateBasePersisterCalled func(path string) (types.Persister, error)
}

// CreateBasePersister -
func (stub *PersisterCreatorStub) CreateBasePersister(path string) (types.Persister, error) {
	if stub.CreateBasePersisterCalled != nil {
		return stub.CreateBasePersisterCalled(path)
	}

	return nil, nil
}

// IsInterfaceNil -
func (stub *PersisterCreatorStub) IsInterfaceNil() bool {
	return stub == nil
}
