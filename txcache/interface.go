package txcache

import (
	"math/big"

	"github.com/multiversx/mx-chain-core-go/data"
	"github.com/multiversx/mx-chain-storage-go/types"
)

// MempoolHost provides blockchain information for mempool operations
type MempoolHost interface {
	ComputeTxFee(tx data.TransactionWithFeeHandler) *big.Int
	GetTransferredValue(tx data.TransactionHandler) *big.Int
	IsInterfaceNil() bool
}

// SelectionSession provides blockchain information for transaction selection
type SelectionSession interface {
	GetAccountState(accountKey []byte) (*types.AccountState, error)
	IsIncorrectlyGuarded(tx data.TransactionHandler) bool
	IsInterfaceNil() bool
}

// ForEachTransaction is an iterator callback
type ForEachTransaction func(txHash []byte, value *WrappedTransaction)
