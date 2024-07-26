package txcache

import (
	"bytes"
	"container/list"
	"sync"

	"github.com/multiversx/mx-chain-core-go/core/atomic"
	"github.com/multiversx/mx-chain-storage-go/common"
)

// txListForSender represents a sorted list of transactions of a particular sender
type txListForSender struct {
	sender            string
	accountNonce      atomic.Uint64
	accountNonceKnown atomic.Flag
	items             *list.List
	totalBytes        atomic.Counter
	constraints       *senderConstraints

	selectionPointer       *list.Element
	selectionPreviousNonce uint64
	selectionDetectedGap   bool

	score             atomic.Uint32
	avgPpuNumerator   float64
	avgPpuDenominator uint64
	noncesTracker     *noncesTracker
	scoreComputer     scoreComputer

	mutex sync.RWMutex
}

type batchSelectionJournal struct {
	selectedNum int
	selectedGas uint64
}

// newTxListForSender creates a new (sorted) list of transactions
func newTxListForSender(sender string, constraints *senderConstraints, scoreComputer scoreComputer) *txListForSender {
	return &txListForSender{
		items:         list.New(),
		sender:        sender,
		constraints:   constraints,
		noncesTracker: newNoncesTracker(),
		scoreComputer: scoreComputer,
	}
}

// AddTx adds a transaction in sender's list
// This is a "sorted" insert
func (listForSender *txListForSender) AddTx(tx *WrappedTransaction, gasHandler TxGasHandler) (bool, [][]byte) {
	// We don't allow concurrent interceptor goroutines to mutate a given sender's list
	listForSender.mutex.Lock()
	defer listForSender.mutex.Unlock()

	insertionPlace, err := listForSender.findInsertionPlace(tx)
	if err != nil {
		return false, nil
	}

	if insertionPlace == nil {
		listForSender.items.PushFront(tx)
	} else {
		listForSender.items.InsertAfter(tx, insertionPlace)
	}

	listForSender.onAddedTransaction(tx, gasHandler)

	// TODO: Check how does the sender get removed if empty afterwards (maybe the answer is: "it never gets empty after applySizeConstraints()").
	evicted := listForSender.applySizeConstraints()
	listForSender.recomputeScore()
	return true, evicted
}

// This function should only be used in critical section (listForSender.mutex)
func (listForSender *txListForSender) applySizeConstraints() [][]byte {
	evictedTxHashes := make([][]byte, 0)

	// Iterate back to front
	for element := listForSender.items.Back(); element != nil; element = element.Prev() {
		if !listForSender.isCapacityExceeded() {
			break
		}

		listForSender.items.Remove(element)
		listForSender.onRemovedListElement(element)

		// Keep track of removed transactions
		value := element.Value.(*WrappedTransaction)
		evictedTxHashes = append(evictedTxHashes, value.TxHash)
	}

	return evictedTxHashes
}

func (listForSender *txListForSender) isCapacityExceeded() bool {
	maxBytes := int64(listForSender.constraints.maxNumBytes)
	maxNumTxs := uint64(listForSender.constraints.maxNumTxs)
	tooManyBytes := listForSender.totalBytes.Get() > maxBytes
	tooManyTxs := listForSender.countTx() > maxNumTxs

	return tooManyBytes || tooManyTxs
}

func (listForSender *txListForSender) onAddedTransaction(tx *WrappedTransaction, gasHandler TxGasHandler) {
	nonce := tx.Tx.GetNonce()
	gasLimit := tx.Tx.GetGasLimit()

	listForSender.totalBytes.Add(tx.Size)
	listForSender.avgPpuNumerator += tx.computeFee(gasHandler)
	listForSender.avgPpuDenominator += gasLimit
	listForSender.noncesTracker.addNonce(nonce)
}

// This function should only be used in critical section (listForSender.mutex)
func (listForSender *txListForSender) recomputeScore() {
	scoreParams := listForSender.getScoreParams()
	score := listForSender.scoreComputer.computeScore(scoreParams)
	listForSender.score.Set(uint32(score))
}

// This function should only be used in critical section (listForSender.mutex)
func (listForSender *txListForSender) getScoreParams() senderScoreParams {
	numTxs := listForSender.countTx()
	minTransactionNonce := uint64(0)
	firstTx := listForSender.getLowestNonceTx()

	if firstTx != nil {
		minTransactionNonce = firstTx.Tx.GetNonce()
	}

	hasSpotlessSequenceOfNonces := listForSender.noncesTracker.isSpotlessSequence(minTransactionNonce, numTxs)

	return senderScoreParams{
		avgPpuNumerator:             listForSender.avgPpuNumerator,
		avgPpuDenominator:           listForSender.avgPpuDenominator,
		isAccountNonceKnown:         listForSender.accountNonceKnown.IsSet(),
		hasSpotlessSequenceOfNonces: hasSpotlessSequenceOfNonces,
	}
}

// This function should only be used in critical section (listForSender.mutex)
func (listForSender *txListForSender) findInsertionPlace(incomingTx *WrappedTransaction) (*list.Element, error) {
	incomingNonce := incomingTx.Tx.GetNonce()
	incomingGasPrice := incomingTx.Tx.GetGasPrice()

	for element := listForSender.items.Back(); element != nil; element = element.Prev() {
		currentTx := element.Value.(*WrappedTransaction)
		currentTxNonce := currentTx.Tx.GetNonce()
		currentTxGasPrice := currentTx.Tx.GetGasPrice()

		if currentTxNonce == incomingNonce {
			if currentTxGasPrice > incomingGasPrice {
				// The incoming transaction will be placed right after the existing one, which has same nonce but higher price.
				// If the nonces are the same, but the incoming gas price is higher or equal, the search loop continues.
				return element, nil
			}
			if currentTxGasPrice == incomingGasPrice {
				// The incoming transaction will be placed right after the existing one, which has same nonce and the same price.
				// (but different hash, because of some other fields like receiver, value or data)
				// This will order out the transactions having the same nonce and gas price

				comparison := bytes.Compare(currentTx.TxHash, incomingTx.TxHash)
				if comparison == 0 {
					// The incoming transaction will be discarded
					return nil, common.ErrItemAlreadyInCache
				}
				if comparison < 0 {
					return element, nil
				}
			}
		}

		if currentTxNonce < incomingNonce {
			// We've found the first transaction with a lower nonce than the incoming one,
			// thus the incoming transaction will be placed right after this one.
			return element, nil
		}
	}

	// The incoming transaction will be inserted at the head of the list.
	return nil, nil
}

// RemoveTx removes a transaction from the sender's list
func (listForSender *txListForSender) RemoveTx(tx *WrappedTransaction) bool {
	// We don't allow concurrent interceptor goroutines to mutate a given sender's list
	listForSender.mutex.Lock()
	defer listForSender.mutex.Unlock()

	marker := listForSender.findListElementWithTx(tx)
	isFound := marker != nil
	if isFound {
		listForSender.items.Remove(marker)
		listForSender.onRemovedListElement(marker)
		listForSender.recomputeScore()
	}

	return isFound
}

func (listForSender *txListForSender) onRemovedListElement(element *list.Element) {
	tx := element.Value.(*WrappedTransaction)
	nonce := tx.Tx.GetNonce()
	gasLimit := tx.Tx.GetGasLimit()

	listForSender.totalBytes.Subtract(tx.Size)
	listForSender.avgPpuNumerator -= tx.TxFee
	listForSender.avgPpuDenominator -= gasLimit
	listForSender.noncesTracker.removeNonce(nonce)
}

// This function should only be used in critical section (listForSender.mutex)
func (listForSender *txListForSender) findListElementWithTx(txToFind *WrappedTransaction) *list.Element {
	txToFindHash := txToFind.TxHash
	txToFindNonce := txToFind.Tx.GetNonce()

	for element := listForSender.items.Front(); element != nil; element = element.Next() {
		value := element.Value.(*WrappedTransaction)
		nonce := value.Tx.GetNonce()

		// Optimization: first, compare nonces, then hashes.
		if nonce == txToFindNonce {
			if bytes.Equal(value.TxHash, txToFindHash) {
				return element
			}
		}

		// Optimization: stop search at this point, since the list is sorted by nonce
		if nonce > txToFindNonce {
			break
		}
	}

	return nil
}

// IsEmpty checks whether the list is empty
func (listForSender *txListForSender) IsEmpty() bool {
	return listForSender.countTxWithLock() == 0
}

// selectBatchTo copies a batch (usually small) of transactions of a limited gas bandwidth and limited number of transactions to a destination slice
// It also updates the internal state used for copy operations
func (listForSender *txListForSender) selectBatchTo(isFirstBatch bool, destination []*WrappedTransaction, numPerBatch int, gasPerBatch uint64) batchSelectionJournal {
	// We can't read from multiple goroutines at the same time
	// And we can't mutate the sender's list while reading it
	listForSender.mutex.Lock()
	defer listForSender.mutex.Unlock()

	journal := batchSelectionJournal{}

	if isFirstBatch {
		// Reset the internal state used for copy operations
		listForSender.selectionPreviousNonce = 0
		listForSender.selectionPointer = listForSender.items.Front()
		listForSender.selectionDetectedGap = listForSender.hasInitialGap()
	}

	// If a nonce gap is detected, no transaction is returned in this read.
	if listForSender.selectionDetectedGap {
		return journal
	}

	selectedGas := uint64(0)
	selectedNum := 0

	for {
		if listForSender.selectionPointer == nil {
			break
		}

		// End because of count
		if selectedNum == numPerBatch || selectedNum == len(destination) {
			break
		}

		// End because of gas limit
		if selectedGas >= gasPerBatch {
			break
		}

		tx := listForSender.selectionPointer.Value.(*WrappedTransaction)
		nonce := tx.Tx.GetNonce()
		gasLimit := tx.Tx.GetGasLimit()

		isMiddleGap := listForSender.selectionPreviousNonce > 0 && nonce > listForSender.selectionPreviousNonce+1
		if isMiddleGap {
			listForSender.selectionDetectedGap = true
			break
		}

		destination[selectedNum] = tx

		listForSender.selectionPreviousNonce = nonce
		listForSender.selectionPointer = listForSender.selectionPointer.Next()

		selectedNum += 1
		selectedGas += gasLimit
	}

	journal.selectedNum = selectedNum
	journal.selectedGas = selectedGas

	return journal
}

// getTxHashes returns the hashes of transactions in the list
func (listForSender *txListForSender) getTxHashes() [][]byte {
	listForSender.mutex.RLock()
	defer listForSender.mutex.RUnlock()

	result := make([][]byte, 0, listForSender.countTx())

	for element := listForSender.items.Front(); element != nil; element = element.Next() {
		value := element.Value.(*WrappedTransaction)
		result = append(result, value.TxHash)
	}

	return result
}

// This function should only be used in critical section (listForSender.mutex)
func (listForSender *txListForSender) countTx() uint64 {
	return uint64(listForSender.items.Len())
}

func (listForSender *txListForSender) countTxWithLock() uint64 {
	listForSender.mutex.RLock()
	defer listForSender.mutex.RUnlock()
	return uint64(listForSender.items.Len())
}

func approximatelyCountTxInLists(lists []*txListForSender) uint64 {
	count := uint64(0)

	for _, listForSender := range lists {
		count += listForSender.countTxWithLock()
	}

	return count
}

// Removes transactions with lower nonces and returns their hashes.
func (listForSender *txListForSender) notifyAccountNonce(nonce uint64) [][]byte {
	// Optimization: if nonce is the same, do nothing.
	if listForSender.accountNonce.Get() == nonce {
		return nil
	}

	listForSender.mutex.Lock()
	defer listForSender.mutex.Unlock()

	listForSender.accountNonce.Set(nonce)
	_ = listForSender.accountNonceKnown.SetReturningPrevious()

	return listForSender.evictTransactionsWithLowerNonces(nonce)
}

// This function should only be used in critical section (listForSender.mutex)
func (listForSender *txListForSender) evictTransactionsWithLowerNonces(accountNonce uint64) [][]byte {
	evictedTxHashes := make([][]byte, 0)

	for element := listForSender.items.Front(); element != nil; element = element.Next() {
		tx := element.Value.(*WrappedTransaction)
		txNonce := tx.Tx.GetNonce()

		if txNonce >= accountNonce {
			break
		}

		listForSender.items.Remove(element)
		listForSender.onRemovedListElement(element)

		// Keep track of removed transactions
		evictedTxHashes = append(evictedTxHashes, tx.TxHash)
	}

	return evictedTxHashes
}

// This function should only be used in critical section (listForSender.mutex)
func (listForSender *txListForSender) hasInitialGap() bool {
	accountNonceKnown := listForSender.accountNonceKnown.IsSet()
	if !accountNonceKnown {
		return false
	}

	firstTx := listForSender.getLowestNonceTx()
	if firstTx == nil {
		return false
	}

	firstTxNonce := firstTx.Tx.GetNonce()
	accountNonce := listForSender.accountNonce.Get()
	hasGap := firstTxNonce > accountNonce
	return hasGap
}

func (listForSender *txListForSender) hasInitialGapWithLock() bool {
	listForSender.mutex.RLock()
	defer listForSender.mutex.RUnlock()
	return listForSender.hasInitialGap()
}

// This function should only be used in critical section (listForSender.mutex)
func (listForSender *txListForSender) getLowestNonceTx() *WrappedTransaction {
	front := listForSender.items.Front()
	if front == nil {
		return nil
	}

	value := front.Value.(*WrappedTransaction)
	return value
}

func (listForSender *txListForSender) getScore() int {
	return int(listForSender.score.Get())
}

// GetKey returns the key
func (listForSender *txListForSender) GetKey() string {
	return listForSender.sender
}
