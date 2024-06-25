package txcache

import (
	"encoding/json"
	"math"
	"os"
	"testing"

	"github.com/multiversx/mx-chain-core-go/core"
	"github.com/multiversx/mx-chain-core-go/data/transaction"
	"github.com/multiversx/mx-chain-core-go/hashing/sha256"
	"github.com/multiversx/mx-chain-core-go/marshal"
	logger "github.com/multiversx/mx-chain-logger-go"
	"github.com/multiversx/mx-chain-storage-go/testscommon/txcachemocks"
	"github.com/stretchr/testify/require"
)

var minPrice = uint64(1000000000)
var minGasLimit = uint64(50000)
var gasProcessingDivisor = uint64(100)

func TestUsage(t *testing.T) {
	_ = logger.SetLogLevel("*:DEBUG")

	config := ConfigSourceMe{
		Name:                       "test",
		NumChunks:                  16,
		NumBytesPerSenderThreshold: maxNumBytesPerSenderUpperBound,
		CountPerSenderThreshold:    math.MaxUint32,
	}

	txGasHandler := &txcachemocks.TxGasHandlerMock{
		MinimumGasMove:       minGasLimit,
		MinimumGasPrice:      minPrice,
		GasProcessingDivisor: gasProcessingDivisor,
	}

	cache, err := NewTxCache(config, txGasHandler)
	require.Nil(t, err)
	require.NotNil(t, cache)

	transactions := loadTransactions(t, "testdata/20240618190000_txcache_0.json")

	for _, tx := range transactions {
		ok, added := cache.AddTx(tx)

		require.True(t, ok)
		require.True(t, added)
	}

	selectedTransactions := cache.SelectTransactionsWithBandwidth(MaxNumOfTxsToSelect, NumTxPerSenderBatchForFillingMiniblock, MaxGasBandwidthPerBatchPerSender)
	require.NotNil(t, selectedTransactions)
}

func loadTransactions(t *testing.T, filePath string) []*WrappedTransaction {
	protoMarshalizer := &marshal.GogoProtoMarshalizer{}
	hasher := sha256.NewSha256()

	frontendTransactions := make([]*transaction.FrontendTransaction, 0)
	transactions := make([]*WrappedTransaction, 0)

	content, err := os.ReadFile(filePath)
	require.Nil(t, err)

	err = json.Unmarshal(content, &frontendTransactions)
	require.Nil(t, err)

	for _, frontendTx := range frontendTransactions {
		senderPubkey, err := addressConverter.Decode(frontendTx.Sender)
		require.Nil(t, err)

		receiverPubkey, err := addressConverter.Decode(frontendTx.Receiver)
		require.Nil(t, err)

		tx := &transaction.Transaction{
			SndAddr:  senderPubkey,
			RcvAddr:  receiverPubkey,
			Nonce:    frontendTx.Nonce,
			Data:     frontendTx.Data,
			GasLimit: frontendTx.GasLimit,
			GasPrice: frontendTx.GasPrice,
		}

		marshalledTx, err := protoMarshalizer.Marshal(tx)
		require.Nil(t, err)

		txHash, err := core.CalculateHash(protoMarshalizer, hasher, tx)
		require.Nil(t, err)

		wrappedTx := &WrappedTransaction{
			Tx:     tx,
			TxHash: txHash,
			Size:   int64(len(marshalledTx)),
		}

		transactions = append(transactions, wrappedTx)
	}

	return transactions
}
