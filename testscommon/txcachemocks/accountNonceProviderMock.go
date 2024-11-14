package txcachemocks

import (
	"errors"
)

// AccountNonceProviderMock -
type AccountNonceProviderMock struct {
	NoncesByAddress       map[string]uint64
	GetAccountNonceCalled func(address []byte) (uint64, error)
}

// NewAccountNonceProviderMock -
func NewAccountNonceProviderMock() *AccountNonceProviderMock {
	return &AccountNonceProviderMock{
		NoncesByAddress: make(map[string]uint64),
	}
}

// GetAccountNonce -
func (stub *AccountNonceProviderMock) GetAccountNonce(address []byte) (uint64, error) {
	if stub.GetAccountNonceCalled != nil {
		return stub.GetAccountNonceCalled(address)
	}

	nonce, ok := stub.NoncesByAddress[string(address)]
	if !ok {
		return 0, errors.New("cannot get nonce")
	}

	return nonce, nil
}

// IsInterfaceNil -
func (stub *AccountNonceProviderMock) IsInterfaceNil() bool {
	return stub == nil
}
