package txcache

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	"github.com/multiversx/mx-chain-core-go/core/pubkeyConverter"
	"github.com/multiversx/mx-chain-core-go/data/transaction"
)

var debuggingPeriod = 6 * time.Second
var addressConverter, _ = pubkeyConverter.NewBech32PubkeyConverter(32, "erd")
var debuggingFolder = "txcache_debugging"
var debuggingFolderSizeLimitInBytes = uint64(5_000_000_000)
var numOldestFilesToRemove = 100

// MaxNumOfTxsToSelect defines the maximum number of transactions that should be selected from the cache
const MaxNumOfTxsToSelect = 30000

// NumTxPerSenderBatchForFillingMiniblock defines the number of transactions to be drawn
// from the transactions pool, for a specific sender, in a single pass.
// Drawing transactions for a miniblock happens in multiple passes, until "MaxItemsInBlock" are drawn.
const NumTxPerSenderBatchForFillingMiniblock = 10

// MaxGasBandwidthPerBatchPerSender defines the maximum gas bandwidth that should be selected for a sender per batch from the cache
const MaxGasBandwidthPerBatchPerSender = 5000000

type debuggingFileInfo struct {
	Name    string
	Size    int64
	ModTime time.Time
}

type dumpedTransaction struct {
	Hash                 string `json:"hash"`
	TxFeeScoreNormalized uint64 `json:"txFeeScoreNormalized"`

	Nonce             uint64 `json:"nonce"`
	Value             string `json:"value"`
	Receiver          string `json:"receiver"`
	Sender            string `json:"sender"`
	SenderUsername    []byte `json:"senderUsername,omitempty"`
	ReceiverUsername  []byte `json:"receiverUsername,omitempty"`
	GasPrice          uint64 `json:"gasPrice"`
	GasLimit          uint64 `json:"gasLimit"`
	Data              []byte `json:"data,omitempty"`
	Signature         string `json:"signature,omitempty"`
	ChainID           string `json:"chainID"`
	Version           uint32 `json:"version"`
	Options           uint32 `json:"options,omitempty"`
	GuardianAddr      string `json:"guardian,omitempty"`
	GuardianSignature string `json:"guardianSignature,omitempty"`
}

type dumpedSender struct {
	Address           string   `json:"address"`
	LastComputedScore uint32   `json:"lastComputedScore"`
	HasInitialGap     bool     `json:"hasInitialGap"`
	IsInGracePeriod   bool     `json:"isInGracePeriod"`
	TotalBytes        uint64   `json:"totalBytes"`
	TotalGas          uint64   `json:"totalGas"`
	TotalFeeScore     uint64   `json:"totalFeeScore"`
	Txs               []string `json:"txs"`
}

func (cache *TxCache) continuouslyDebug() {
	// Create folder if it doesn't exist:
	err := os.MkdirAll(debuggingFolder, 0755)
	if err != nil {
		log.Error("error creating debugging folder", "error", err)
	}

	func() {
		for {
			cache.saveTransactionsToFile()

			err := limitDebuggingFiles(debuggingFolder)
			if err != nil {
				log.Error("error limiting debugging files", "error", err)
			}

			// Simulate selection
			cache.SelectTransactionsWithBandwidth(MaxNumOfTxsToSelect, NumTxPerSenderBatchForFillingMiniblock, MaxGasBandwidthPerBatchPerSender)

			time.Sleep(debuggingPeriod)
		}
	}()
}

func (cache *TxCache) saveTransactionsToFile() {
	timestamp := time.Now().Format("20060102150405")
	outfileTxs := path.Join(debuggingFolder, fmt.Sprintf("%s_txcache_%s.json", timestamp, cache.name))
	outfileSenders := path.Join(debuggingFolder, fmt.Sprintf("%s_txcache_senders_%s.json", timestamp, cache.name))

	numTxs := cache.txByHash.counter.Get()
	numSenders := cache.txListBySender.counter.Get()
	allTxs := make([]*dumpedTransaction, 0, numTxs)
	allSenders := make([]*dumpedSender, 0, numSenders)

	cache.txByHash.forEach(func(txHash []byte, wrappedTx *WrappedTransaction) {
		dumpedTx := wrappedTxToDumpedTx(txHash, wrappedTx)
		allTxs = append(allTxs, dumpedTx)
	})

	sendersLists := cache.txListBySender.getSnapshotDescending()

	for _, senderList := range sendersLists {
		dumpedSender := txListForSenderToDumpedSender(senderList)
		allSenders = append(allSenders, dumpedSender)
	}

	err := saveJson(outfileTxs, allTxs)
	if err != nil {
		log.Error("error saving txs to file", "error", err)
	}

	err = saveJson(outfileSenders, allSenders)
	if err != nil {
		log.Error("error saving senders to file", "error", err)
	}
}

func wrappedTxToDumpedTx(txHash []byte, wrappedTx *WrappedTransaction) *dumpedTransaction {
	rawTx := wrappedTx.Tx.(*transaction.Transaction)

	receiver, _ := addressConverter.Encode(rawTx.RcvAddr)
	sender, _ := addressConverter.Encode(rawTx.SndAddr)
	guardian, _ := addressConverter.Encode(rawTx.GuardianAddr)

	value := ""
	if rawTx.Value != nil {
		value = rawTx.Value.String()
	}

	dumpedTx := &dumpedTransaction{
		Hash:                 hex.EncodeToString(txHash),
		TxFeeScoreNormalized: wrappedTx.TxFeeScoreNormalized,

		Nonce:             rawTx.Nonce,
		Value:             value,
		Receiver:          receiver,
		Sender:            sender,
		SenderUsername:    rawTx.SndUserName,
		ReceiverUsername:  rawTx.RcvUserName,
		GasPrice:          rawTx.GasPrice,
		GasLimit:          rawTx.GasLimit,
		Data:              rawTx.Data,
		Signature:         hex.EncodeToString(rawTx.Signature),
		ChainID:           string(rawTx.ChainID),
		Version:           rawTx.Version,
		Options:           rawTx.Options,
		GuardianAddr:      guardian,
		GuardianSignature: hex.EncodeToString(rawTx.GuardianSignature),
	}

	return dumpedTx
}

func txListForSenderToDumpedSender(sender *txListForSender) *dumpedSender {
	address, _ := addressConverter.Encode([]byte(sender.sender))
	txsHashes := sender.getTxHashes()

	txsHashesStrings := make([]string, len(txsHashes))

	for i, txHash := range txsHashes {
		txsHashesStrings[i] = hex.EncodeToString(txHash)
	}

	return &dumpedSender{
		Address:           address,
		LastComputedScore: sender.getLastComputedScore(),
		HasInitialGap:     sender.hasInitialGap(),
		IsInGracePeriod:   sender.isInGracePeriod(),
		TotalBytes:        sender.totalBytes.GetUint64(),
		TotalGas:          sender.totalGas.GetUint64(),
		TotalFeeScore:     sender.totalFeeScore.GetUint64(),
		Txs:               txsHashesStrings,
	}
}

func saveJson(outfile string, data interface{}) error {
	log.Info("saving debugging data", "file", outfile, "data", data)

	outcomeJSON, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	err = os.WriteFile(outfile, outcomeJSON, 0644)
	if err != nil {
		return err
	}

	return nil
}

func limitDebuggingFiles(directory string) error {
	var files []debuggingFileInfo
	var totalSize uint64

	err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			files = append(files, debuggingFileInfo{Name: path, Size: info.Size(), ModTime: info.ModTime()})
			totalSize += uint64(info.Size())
		}

		return nil
	})

	if err != nil {
		return err
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].ModTime.Before(files[j].ModTime)
	})

	// If size is exceeded, remove some old files.
	if totalSize < debuggingFolderSizeLimitInBytes {
		return nil
	}

	if len(files) < numOldestFilesToRemove {
		return fmt.Errorf("not enough files to start removal, only %d files found", len(files))
	}

	for i := 0; i < numOldestFilesToRemove; i++ {
		log.Info("removing old debugging file", "file", files[i].Name)

		err = os.Remove(files[i].Name)
		if err != nil {
			return err
		}
	}

	return nil
}
