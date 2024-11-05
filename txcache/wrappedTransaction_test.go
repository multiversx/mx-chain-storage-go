package txcache

import (
	"testing"

	"github.com/multiversx/mx-chain-storage-go/testscommon/txcachemocks"
	"github.com/stretchr/testify/require"
)

func Test_estimateTxFeeScore(t *testing.T) {
	txGasHandler, txFeeHelper := dummyParamsWithGasPrice(oneBillion)
	A := createTxWithParams([]byte("a"), "a", 1, 200, 50_000, oneBillion)
	B := createTxWithParams([]byte("b"), "b", 1, 200, 50_000_000, oneBillion)
	C := createTxWithParams([]byte("c"), "c", 1, 200, 500_000_000, oneBillion)
	D := createTxWithParams([]byte("d"), "d", 1, 200, 500_000_000, 2*oneBillion)
	E := createTxWithParams([]byte("e"), "e", 1, 200, 51_000, 2*oneBillion)

	scoreA := estimateTxFeeScore(A, txGasHandler, txFeeHelper)
	scoreB := estimateTxFeeScore(B, txGasHandler, txFeeHelper)
	scoreC := estimateTxFeeScore(C, txGasHandler, txFeeHelper)
	scoreD := estimateTxFeeScore(D, txGasHandler, txFeeHelper)
	scoreE := estimateTxFeeScore(E, txGasHandler, txFeeHelper)

	require.Equal(t, 11436, int(scoreA))
	require.Equal(t, 11436, int(A.TxFeeScoreNormalized))
	require.Equal(t, 8791116, int(scoreB))
	require.Equal(t, 8791116, int(B.TxFeeScoreNormalized))
	require.Equal(t, 87893196, int(scoreC))
	require.Equal(t, 87893196, int(C.TxFeeScoreNormalized))
	require.Equal(t, 92786964, int(scoreD))
	require.Equal(t, 92786964, int(D.TxFeeScoreNormalized))
	require.Equal(t, 22884, int(scoreE))
	require.Equal(t, 22884, int(E.TxFeeScoreNormalized))
}

func Test_normalizeGasPriceProcessing(t *testing.T) {
	txGasHandler, txFeeHelper := dummyParamsWithGasPriceAndDivisor(100*oneBillion, 100)
	A := createTxWithParams([]byte("A"), "a", 1, 200, 1500000000, 100*oneBillion)
	normalizedGasPriceProcess := normalizeGasPriceProcessing(A, txGasHandler, txFeeHelper)
	require.Equal(t, uint64(7), normalizedGasPriceProcess)

	txGasHandler, txFeeHelper = dummyParamsWithGasPriceAndDivisor(100*oneBillion, 50)
	normalizedGasPriceProcess = normalizeGasPriceProcessing(A, txGasHandler, txFeeHelper)
	require.Equal(t, uint64(14), normalizedGasPriceProcess)

	txGasHandler, txFeeHelper = dummyParamsWithGasPriceAndDivisor(100*oneBillion, 1)
	normalizedGasPriceProcess = normalizeGasPriceProcessing(A, txGasHandler, txFeeHelper)
	require.Equal(t, uint64(745), normalizedGasPriceProcess)

	txGasHandler, txFeeHelper = dummyParamsWithGasPriceAndDivisor(100000, 100)
	A = createTxWithParams([]byte("A"), "a", 1, 200, 1500000000, 100000)
	normalizedGasPriceProcess = normalizeGasPriceProcessing(A, txGasHandler, txFeeHelper)
	require.Equal(t, uint64(7), normalizedGasPriceProcess)
}

func Test_computeProcessingGasPriceAdjustment(t *testing.T) {
	txGasHandler, txFeeHelper := dummyParamsWithGasPriceAndDivisor(100*oneBillion, 100)
	A := createTxWithParams([]byte("A"), "a", 1, 200, 1500000000, 100*oneBillion)
	adjustment := computeProcessingGasPriceAdjustment(A, txGasHandler, txFeeHelper)
	require.Equal(t, uint64(80), adjustment)

	A = createTxWithParams([]byte("A"), "a", 1, 200, 1500000000, 150*oneBillion)
	adjustment = computeProcessingGasPriceAdjustment(A, txGasHandler, txFeeHelper)
	expectedAdjustment := float64(100) * processFeeFactor / float64(1.5)
	require.Equal(t, uint64(expectedAdjustment), adjustment)

	A = createTxWithParams([]byte("A"), "a", 1, 200, 1500000000, 110*oneBillion)
	adjustment = computeProcessingGasPriceAdjustment(A, txGasHandler, txFeeHelper)
	expectedAdjustment = float64(100) * processFeeFactor / float64(1.1)
	require.Equal(t, uint64(expectedAdjustment), adjustment)
}

func dummyParamsWithGasPriceAndDivisor(minGasPrice, processingPriceDivisor uint64) (TxGasHandler, feeHelper) {
	minPrice := minGasPrice
	minPriceProcessing := minGasPrice / processingPriceDivisor
	minGasLimit := uint64(50000)
	txFeeHelper := newFeeComputationHelper(minPrice, minGasLimit, minPriceProcessing)
	txGasHandler := &txcachemocks.TxGasHandlerMock{
		MinimumGasMove:       minGasLimit,
		MinimumGasPrice:      minPrice,
		GasProcessingDivisor: processingPriceDivisor,
	}
	return txGasHandler, txFeeHelper
}
