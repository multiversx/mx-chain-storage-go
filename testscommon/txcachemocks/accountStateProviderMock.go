package txcachemocks

import (
	"math/big"

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

// SetNonce -
func (mock *AccountStateProviderMock) SetNonce(address []byte, nonce uint64) {
	key := string(address)

	if mock.AccountStateByAddress[key] == nil {
		mock.AccountStateByAddress[key] = newDefaultAccountState()
	}

	mock.AccountStateByAddress[key].Nonce = nonce
}

// SetBalance -
func (mock *AccountStateProviderMock) SetBalance(address []byte, balance *big.Int) {
	key := string(address)

	if mock.AccountStateByAddress[key] == nil {
		mock.AccountStateByAddress[key] = newDefaultAccountState()
	}

	mock.AccountStateByAddress[key].Balance = balance
}

// SetGuardian -
func (mock *AccountStateProviderMock) SetGuardian(address []byte, guardian []byte) {
	key := string(address)

	if mock.AccountStateByAddress[key] == nil {
		mock.AccountStateByAddress[key] = newDefaultAccountState()
	}

	mock.AccountStateByAddress[key].Guardian = guardian
}

// GetAccountState -
func (mock *AccountStateProviderMock) GetAccountState(address []byte) (*types.AccountState, error) {
	if mock.GetAccountStateCalled != nil {
		return mock.GetAccountStateCalled(address)
	}

	state, ok := mock.AccountStateByAddress[string(address)]
	if ok {
		return state, nil
	}

	return newDefaultAccountState(), nil
}

// IsInterfaceNil -
func (mock *AccountStateProviderMock) IsInterfaceNil() bool {
	return mock == nil
}

func newDefaultAccountState() *types.AccountState {
	return &types.AccountState{
		Nonce:    0,
		Balance:  big.NewInt(1000000000000000000),
		Guardian: nil,
	}
}
