package types

import "math/big"

// AccountState represents the state of an account, as seen by the mempool
type AccountState struct {
	Nonce    uint64
	Balance  *big.Int
	Guardian []byte
}
