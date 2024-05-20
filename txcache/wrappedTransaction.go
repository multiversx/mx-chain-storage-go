package txcache

import (
	"bytes"

	"github.com/multiversx/mx-chain-core-go/data"
	"github.com/multiversx/mx-chain-core-go/data/transaction"
)

// WrappedTransaction contains a transaction, its hash and extra information
type WrappedTransaction struct {
	Tx                   data.TransactionHandler
	TxDirectPointer      *transaction.Transaction
	TxHash               []byte
	SenderShardID        uint32
	ReceiverShardID      uint32
	Size                 int64
	TxFeeScoreNormalized uint64
}

func (wrappedTx *WrappedTransaction) sameAs(another *WrappedTransaction) bool {
	return bytes.Equal(wrappedTx.TxHash, another.TxHash)
}
