package txcachemocks

import (
	"math/big"

	"github.com/multiversx/mx-chain-core-go/core"
	"github.com/multiversx/mx-chain-core-go/data"
)

// MempoolHostMock -
type MempoolHostMock struct {
	minGasLimit      uint64
	minGasPrice      uint64
	gasPerDataByte   uint64
	gasPriceModifier float64
}

// NewMempoolHostMock -
func NewMempoolHostMock() *MempoolHostMock {
	return &MempoolHostMock{
		minGasLimit:      50000,
		minGasPrice:      1000000000,
		gasPerDataByte:   1500,
		gasPriceModifier: 0.01,
	}
}

// WithGasPriceModifier -
func (mock *MempoolHostMock) WithGasPriceModifier(gasPriceModifier float64) *MempoolHostMock {
	mock.gasPriceModifier = gasPriceModifier
	return mock
}

// ComputeTxFee -
func (mock *MempoolHostMock) ComputeTxFee(tx data.TransactionWithFeeHandler) *big.Int {
	dataLength := uint64(len(tx.GetData()))
	gasPriceForMovement := tx.GetGasPrice()
	gasPriceForProcessing := uint64(float64(gasPriceForMovement) * mock.gasPriceModifier)

	gasLimitForMovement := mock.minGasLimit + dataLength*mock.gasPerDataByte
	if tx.GetGasLimit() < gasLimitForMovement {
		panic("tx.GetGasLimit() < gasLimitForMovement")
	}

	gasLimitForProcessing := tx.GetGasLimit() - gasLimitForMovement
	feeForMovement := core.SafeMul(gasPriceForMovement, gasLimitForMovement)
	feeForProcessing := core.SafeMul(gasPriceForProcessing, gasLimitForProcessing)
	fee := big.NewInt(0).Add(feeForMovement, feeForProcessing)
	return fee
}

// IsInterfaceNil -
func (mock *MempoolHostMock) IsInterfaceNil() bool {
	return mock == nil
}
