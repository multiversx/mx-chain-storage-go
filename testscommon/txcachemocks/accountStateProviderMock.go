package txcachemocks

import (
	"math/big"
	"sync"

	"github.com/multiversx/mx-chain-core-go/data"
	"github.com/multiversx/mx-chain-storage-go/types"
)

// AccountStateProviderMock -
type AccountStateProviderMock struct {
	mutex sync.Mutex

	AccountStateByAddress map[string]*types.AccountState
	GetAccountStateCalled func(address []byte) (*types.AccountState, error)
	IsBadlyGuardedCalled  func(tx data.TransactionHandler) bool
}

// NewAccountStateProviderMock -
func NewAccountStateProviderMock() *AccountStateProviderMock {
	return &AccountStateProviderMock{
		AccountStateByAddress: make(map[string]*types.AccountState),
	}
}

// SetNonce -
func (mock *AccountStateProviderMock) SetNonce(address []byte, nonce uint64) {
	mock.mutex.Lock()
	defer mock.mutex.Unlock()

	key := string(address)

	if mock.AccountStateByAddress[key] == nil {
		mock.AccountStateByAddress[key] = newDefaultAccountState()
	}

	mock.AccountStateByAddress[key].Nonce = nonce
}

// SetBalance -
func (mock *AccountStateProviderMock) SetBalance(address []byte, balance *big.Int) {
	mock.mutex.Lock()
	defer mock.mutex.Unlock()

	key := string(address)

	if mock.AccountStateByAddress[key] == nil {
		mock.AccountStateByAddress[key] = newDefaultAccountState()
	}

	mock.AccountStateByAddress[key].Balance = balance
}

// GetAccountState -
func (mock *AccountStateProviderMock) GetAccountState(address []byte) (*types.AccountState, error) {
	mock.mutex.Lock()
	defer mock.mutex.Unlock()

	if mock.GetAccountStateCalled != nil {
		return mock.GetAccountStateCalled(address)
	}

	state, ok := mock.AccountStateByAddress[string(address)]
	if ok {
		return state, nil
	}

	return newDefaultAccountState(), nil
}

// IsBadlyGuarded -
func (mock *AccountStateProviderMock) IsBadlyGuarded(tx data.TransactionHandler) bool {
	if mock.IsBadlyGuardedCalled != nil {
		return mock.IsBadlyGuardedCalled(tx)
	}

	return false
}

// IsInterfaceNil -
func (mock *AccountStateProviderMock) IsInterfaceNil() bool {
	return mock == nil
}

func newDefaultAccountState() *types.AccountState {
	return &types.AccountState{
		Nonce:   0,
		Balance: big.NewInt(1000000000000000000),
	}
}
