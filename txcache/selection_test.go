package txcache

import (
	"fmt"
	"testing"

	"github.com/multiversx/mx-chain-core-go/core"
	"github.com/stretchr/testify/require"
)

func Test_mergeTwoBunches(t *testing.T) {
	t.Run("empty bunches", func(t *testing.T) {
		merged := mergeTwoBunches(BunchOfTransactions{}, BunchOfTransactions{})
		require.Len(t, merged, 0)
	})

	t.Run("alice and bob (1)", func(t *testing.T) {
		first := BunchOfTransactions{
			createTx([]byte("hash-alice-1"), "alice", 1).withGasPrice(42),
		}

		second := BunchOfTransactions{
			createTx([]byte("hash-bob-1"), "bob", 1).withGasPrice(43),
		}

		merged := mergeTwoBunches(first, second)

		require.Len(t, merged, 2)
		require.Equal(t, "hash-bob-1", string(merged[0].TxHash))
		require.Equal(t, "hash-alice-1", string(merged[1].TxHash))
	})
}

func Test_mergeBunches(t *testing.T) {
	sw := core.NewStopWatch()

	t.Run("numSenders = 1000, numTransactions = 1000", func(t *testing.T) {
		bunches := createBunchesOfTransactionsWithUniformDistribution(1000, 1000)

		sw.Start(t.Name())
		merged := mergeBunches(bunches)
		sw.Stop(t.Name())

		require.Len(t, merged, 1000*1000)
	})

	t.Run("numSenders = 1000, numTransactions = 1000, parallel (4 jobs)", func(t *testing.T) {
		bunches := createBunchesOfTransactionsWithUniformDistribution(1000, 1000)

		sw.Start(t.Name())
		merged := mergeBunchesInParallel(bunches, 4)
		sw.Stop(t.Name())

		require.Len(t, merged, 1000*1000)
	})

	for name, measurement := range sw.GetMeasurementsMap() {
		fmt.Printf("%fs (%s)\n", measurement, name)
	}
}

func TestTxCache_selectTransactionsFromBunchesUsingHeap(t *testing.T) {
	sw := core.NewStopWatch()

	t.Run("numSenders = 1000, numTransactions = 1000", func(t *testing.T) {
		bunches := createBunchesOfTransactionsWithUniformDistribution(1000, 1000)

		sw.Start(t.Name())
		merged := selectTransactionsFromBunchesUsingHeap(bunches, 10_000_000_000)
		sw.Stop(t.Name())

		require.Equal(t, 200001, len(merged))
	})

	for name, measurement := range sw.GetMeasurementsMap() {
		fmt.Printf("%fs (%s)\n", measurement, name)
	}
}

func createBunchesOfTransactionsWithUniformDistribution(nSenders int, nTransactionsPerSender int) []BunchOfTransactions {
	bunches := make([]BunchOfTransactions, 0, nSenders)

	for senderTag := 0; senderTag < nSenders; senderTag++ {
		bunch := make(BunchOfTransactions, 0, nTransactionsPerSender)
		sender := createFakeSenderAddress(senderTag)

		for txNonce := nTransactionsPerSender; txNonce > 0; txNonce-- {
			transactionHash := createFakeTxHash(sender, txNonce)
			transaction := createTx(transactionHash, string(sender), uint64(txNonce))
			bunch = append(bunch, transaction)
		}

		bunches = append(bunches, bunch)
	}

	return bunches
}