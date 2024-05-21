package txcache

import (
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
	log.Debug("TxCache: selection ended", "name", cache.name, "duration", duration, "numTxSelected", len(selection))
}
