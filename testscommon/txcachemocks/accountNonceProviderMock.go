package txcachemocks

import (
	"errors"
)

type accountNonceProviderMock struct {
	GetAccountNonceCalled func(address []byte) (uint64, error)
}

// NewAccountNonceProviderMock -
func NewAccountNonceProviderMock() *accountNonceProviderMock {
	return &accountNonceProviderMock{}
}

// GetAccountNonce -
func (stub *accountNonceProviderMock) GetAccountNonce(address []byte) (uint64, error) {
	if stub.GetAccountNonceCalled != nil {
		return stub.GetAccountNonceCalled(address)
	}

	return 0, errors.New("GetAccountNonceCalled is not set")
}

// IsInterfaceNil -
func (stub *accountNonceProviderMock) IsInterfaceNil() bool {
	return stub == nil
}
