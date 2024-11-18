package txcachemocks

import (
	"errors"

	"github.com/multiversx/mx-chain-storage-go/types"
)

// AccountStateProviderMock -
type AccountStateProviderMock struct {
	AccountStateByAddress map[string]*types.AccountState
	GetAccountStateCalled func(address []byte) (*types.AccountState, error)
}

// NewAccountStateProviderMock -
func NewAccountStateProviderMock() *AccountStateProviderMock {
	return &AccountStateProviderMock{
		AccountStateByAddress: make(map[string]*types.AccountState),
	}
}

// GetAccountState -
func (stub *AccountStateProviderMock) GetAccountState(address []byte) (*types.AccountState, error) {
	if stub.GetAccountStateCalled != nil {
		return stub.GetAccountStateCalled(address)
	}

	state, ok := stub.AccountStateByAddress[string(address)]
	if !ok {
		return nil, errors.New("cannot get state")
	}

	return state, nil
}

// IsInterfaceNil -
func (stub *AccountStateProviderMock) IsInterfaceNil() bool {
	return stub == nil
}
