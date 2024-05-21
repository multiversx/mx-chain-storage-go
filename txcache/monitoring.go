package txcache

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/multiversx/mx-chain-core-go/core"
	logger "github.com/multiversx/mx-chain-logger-go"
)

var log = logger.GetOrCreate("txcache")

func (cache *TxCache) monitorSelectionStart() *core.StopWatch {
	log.Debug("TxCache: selection started", "name", cache.name, "numBytes", cache.NumBytes(), "txs", cache.CountTx(), "senders", cache.CountSenders())
	sw := core.NewStopWatch()
	sw.Start("selection")
	return sw
}

func (cache *TxCache) monitorSelectionEnd(selection []*WrappedTransaction, stopWatch *core.StopWatch) {
	stopWatch.Stop("selection")
	duration := stopWatch.GetMeasurement("selection")
	numSendersSelected := cache.numSendersSelected.Reset()
	numSendersWithInitialGap := cache.numSendersWithInitialGap.Reset()
	numSendersWithMiddleGap := cache.numSendersWithMiddleGap.Reset()
	numSendersInGracePeriod := cache.numSendersInGracePeriod.Reset()

	log.Debug("TxCache: selection ended", "name", cache.name, "duration", duration,
		"numTxSelected", len(selection),
		"numSendersSelected", numSendersSelected,
		"numSendersWithInitialGap", numSendersWithInitialGap,
		"numSendersWithMiddleGap", numSendersWithMiddleGap,
		"numSendersInGracePeriod", numSendersInGracePeriod,
	)
}

type batchSelectionJournal struct {
	copied        int
	isFirstBatch  bool
	hasInitialGap bool
	hasMiddleGap  bool
	isGracePeriod bool
}

func (cache *TxCache) monitorBatchSelectionEnd(journal batchSelectionJournal) {
	if !journal.isFirstBatch {
		return
	}

	if journal.hasInitialGap {
		cache.numSendersWithInitialGap.Increment()
	} else if journal.hasMiddleGap {
		// Currently, we only count middle gaps on first batch (for simplicity)
		cache.numSendersWithMiddleGap.Increment()
	}

	if journal.isGracePeriod {
		cache.numSendersInGracePeriod.Increment()
	} else if journal.copied > 0 {
		cache.numSendersSelected.Increment()
	}
}

type internalConsistencyJournal struct {
	numInMapByHash        int
	numInMapBySender      int
	numMissingInMapByHash int
}

func (journal *internalConsistencyJournal) isFine() bool {
	return (journal.numInMapByHash == journal.numInMapBySender) && (journal.numMissingInMapByHash == 0)
}

func (journal *internalConsistencyJournal) display() {
	log.Debug("TxCache.internalConsistencyJournal:", "fine", journal.isFine(), "numInMapByHash", journal.numInMapByHash, "numInMapBySender", journal.numInMapBySender, "numMissingInMapByHash", journal.numMissingInMapByHash)
}

func (cache *TxCache) displaySendersSummary() {
	if log.GetLevel() != logger.LogTrace {
		return
	}

	senders := cache.txListBySender.getSnapshotAscending()
	if len(senders) == 0 {
		return
	}

	var builder strings.Builder
	builder.WriteString("\n[#index (score)] address [nonce known / nonce vs lowestTxNonce] txs = numTxs, !numFailedSelections\n")

	for i, sender := range senders {
		address := hex.EncodeToString([]byte(sender.sender))
		accountNonce := sender.accountNonce.Get()
		accountNonceKnown := sender.accountNonceKnown.IsSet()
		numFailedSelections := sender.numFailedSelections.Get()
		score := sender.getLastComputedScore()
		numTxs := sender.countTxWithLock()

		lowestTxNonce := -1
		lowestTx := sender.getLowestNonceTx()
		if lowestTx != nil {
			lowestTxNonce = int(lowestTx.Tx.GetNonce())
		}

		_, _ = fmt.Fprintf(&builder, "[#%d (%d)] %s [%t / %d vs %d] txs = %d, !%d\n", i, score, address, accountNonceKnown, accountNonce, lowestTxNonce, numTxs, numFailedSelections)
	}

	summary := builder.String()
	log.Debug("TxCache.displaySendersSummary()", "name", cache.name, "summary\n", summary)
}
