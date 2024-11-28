package txcachemocks

import (
	"math/big"
	"sync"

	"github.com/multiversx/mx-chain-core-go/data"
	"github.com/multiversx/mx-chain-storage-go/types"
)

// SelectionSessionMock -
type SelectionSessionMock struct {
	mutex sync.Mutex

	AccountStateByAddress      map[string]*types.AccountState
	GetAccountStateCalled      func(address []byte) (*types.AccountState, error)
	IsIncorrectlyGuardedCalled func(tx data.TransactionHandler) bool
	GetTransferredValueCalled  func(tx data.TransactionHandler) *big.Int
}

// NewSelectionSessionMock -
func NewSelectionSessionMock() *SelectionSessionMock {
	return &SelectionSessionMock{
		AccountStateByAddress: make(map[string]*types.AccountState),
	}
}

// SetNonce -
func (mock *SelectionSessionMock) SetNonce(address []byte, nonce uint64) {
	mock.mutex.Lock()
	defer mock.mutex.Unlock()

	key := string(address)

	if mock.AccountStateByAddress[key] == nil {
		mock.AccountStateByAddress[key] = newDefaultAccountState()
	}

	mock.AccountStateByAddress[key].Nonce = nonce
}

// SetBalance -
func (mock *SelectionSessionMock) SetBalance(address []byte, balance *big.Int) {
	mock.mutex.Lock()
	defer mock.mutex.Unlock()

	key := string(address)

	if mock.AccountStateByAddress[key] == nil {
		mock.AccountStateByAddress[key] = newDefaultAccountState()
	}

	mock.AccountStateByAddress[key].Balance = balance
}

// GetAccountState -
func (mock *SelectionSessionMock) GetAccountState(address []byte) (*types.AccountState, error) {
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

// IsIncorrectlyGuarded -
func (mock *SelectionSessionMock) IsIncorrectlyGuarded(tx data.TransactionHandler) bool {
	if mock.IsIncorrectlyGuardedCalled != nil {
		return mock.IsIncorrectlyGuardedCalled(tx)
	}

	return false
}

// GetTransferredValue -
func (mock *SelectionSessionMock) GetTransferredValue(tx data.TransactionHandler) *big.Int {
	if mock.GetTransferredValueCalled != nil {
		return mock.GetTransferredValueCalled(tx)
	}

	return tx.GetValue()
}

// IsInterfaceNil -
func (mock *SelectionSessionMock) IsInterfaceNil() bool {
	return mock == nil
}

func newDefaultAccountState() *types.AccountState {
	return &types.AccountState{
		Nonce:   0,
		Balance: big.NewInt(1000000000000000000),
	}
}
