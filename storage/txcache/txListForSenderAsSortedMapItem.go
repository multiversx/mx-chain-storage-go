package txcache

import (
	"math"

	"github.com/ElrondNetwork/elrond-go/storage/txcache/maps"
)

type senderScoreParams struct {
	count uint64
	// Size is in bytes
	size uint64
	// Fee is in micro ERD
	fee uint64
	gas uint64
}

// GetKey return the key
func (listForSender *txListForSender) GetKey() string {
	return listForSender.sender
}

func (listForSender *txListForSender) getLastComputedScore() uint32 {
	return listForSender.lastComputedScore.Get()
}

// ComputeScore computes the score of the sender, as an integer 0-100
func (listForSender *txListForSender) ComputeScore() uint32 {
	score := uint32(listForSender.computeRawScore())
	listForSender.lastComputedScore.Set(score)
	return score
}

func (listForSender *txListForSender) computeRawScore() float64 {
	fee := listForSender.totalFee.GetUint64()
	gas := listForSender.totalGas.GetUint64()
	size := listForSender.totalBytes.GetUint64()
	count := listForSender.countTx()

	return computeSenderScore(senderScoreParams{count: count, size: size, fee: fee, gas: gas})
}

func computeSenderScore(params senderScoreParams) float64 {
	allParamsDefined := params.fee > 0 && params.gas > 0 && params.size > 0 && params.count > 0
	if !allParamsDefined {
		return 0
	}

	// PPU (price per gas unit) is in micro ERD
	// TODO-TXCACHE get from economics config
	const PPUMin = float64(100)
	PPUAvg := float64(params.fee) / float64(params.gas)
	PPUScore := math.Pow(PPUAvg/PPUMin, 3)

	countPow2 := float64(params.count) * float64(params.count)
	countScore := math.Log(countPow2+1) + 1

	// We use size in ~kB
	size := float64(params.size) / 1000
	sizePow2 := float64(size) * float64(size)
	sizeScore := math.Log(sizePow2+1) + 1

	rawScore := PPUScore / countScore / sizeScore
	// We apply the logistic function,
	// and then subtract 0.5, since we only deal with positive scores,
	// and then we multiply by 2, to have full [0..1] range.
	asymptoticScore := (1/(1+math.Exp(-rawScore)) - 0.5) * 2
	score := asymptoticScore * float64(numberOfScoreChunks)
	return score
}

// GetScoreChunk returns the score chunk the sender is currently in
func (listForSender *txListForSender) GetScoreChunk() *maps.MapChunk {
	return listForSender.scoreChunk
}

// GetScoreChunk returns the score chunk the sender is currently in
func (listForSender *txListForSender) SetScoreChunk(scoreChunk *maps.MapChunk) {
	listForSender.scoreChunk = scoreChunk
}
