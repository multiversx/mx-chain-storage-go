package txcache

import (
	"bytes"
	"math/big"

	"github.com/multiversx/mx-chain-storage-go/types"
)

type transactionsHeapItem struct {
	sender []byte
	bunch  bunchOfTransactions

	// Whether the sender's state has been requested within a selection session.
	senderStateRequested bool
	// Whether the sender's state has been requested and provided (with success) within a selection session.
	senderStateProvided bool
	// The sender's state (if requested and provided).
	senderState *types.AccountState

	currentTransactionIndex        int
	currentTransaction             *WrappedTransaction
	currentTransactionNonce        uint64
	latestSelectedTransaction      *WrappedTransaction
	latestSelectedTransactionNonce uint64

	accumulatedFee *big.Int
}

func newTransactionsHeapItem(bunch bunchOfTransactions) (*transactionsHeapItem, error) {
	if len(bunch) == 0 {
		return nil, errEmptyBunchOfTransactions
	}

	firstTransaction := bunch[0]

	return &transactionsHeapItem{
		sender: firstTransaction.Tx.GetSndAddr(),
		bunch:  bunch,

		senderStateRequested: false,
		senderStateProvided:  false,
		senderState:          nil,

		currentTransactionIndex:   0,
		currentTransaction:        firstTransaction,
		currentTransactionNonce:   firstTransaction.Tx.GetNonce(),
		latestSelectedTransaction: nil,

		accumulatedFee: big.NewInt(0),
	}, nil
}

func (item *transactionsHeapItem) selectTransaction() *WrappedTransaction {
	item.accumulateFee()

	item.latestSelectedTransaction = item.currentTransaction
	item.latestSelectedTransactionNonce = item.currentTransactionNonce

	return item.currentTransaction
}

func (item *transactionsHeapItem) accumulateFee() {
	fee := item.currentTransaction.Fee.Load()
	if fee == nil {
		// This should never happen during selection.
		return
	}

	item.accumulatedFee.Add(item.accumulatedFee, fee)
}

func (item *transactionsHeapItem) gotoNextTransaction() bool {
	if item.currentTransactionIndex+1 >= len(item.bunch) {
		return false
	}

	item.currentTransactionIndex++
	item.currentTransaction = item.bunch[item.currentTransactionIndex]
	item.currentTransactionNonce = item.currentTransaction.Tx.GetNonce()
	return true
}

func (item *transactionsHeapItem) detectInitialGap() bool {
	if item.latestSelectedTransaction != nil {
		return false
	}
	if !item.senderStateProvided {
		// This should never happen during selection.
		return false
	}

	hasInitialGap := item.currentTransactionNonce > item.senderState.Nonce
	if hasInitialGap {
		logSelect.Trace("transactionsHeapItem.detectGap, initial gap",
			"tx", item.currentTransaction.TxHash,
			"nonce", item.currentTransactionNonce,
			"sender", item.sender,
			"senderState.Nonce", item.senderState.Nonce,
		)
	}

	return hasInitialGap
}

func (item *transactionsHeapItem) detectMiddleGap() bool {
	if item.latestSelectedTransaction == nil {
		return false
	}

	// Detect middle gap.
	hasMiddleGap := item.currentTransactionNonce > item.latestSelectedTransactionNonce+1
	if hasMiddleGap {
		logSelect.Trace("transactionsHeapItem.detectGap, middle gap",
			"tx", item.currentTransaction.TxHash,
			"nonce", item.currentTransactionNonce,
			"sender", item.sender,
			"previousSelectedNonce", item.latestSelectedTransactionNonce,
		)
	}

	return hasMiddleGap
}

func (item *transactionsHeapItem) detectWillFeeExceedBalance() bool {
	if !item.senderStateProvided {
		// This should never happen during selection.
		return false
	}

	senderBalance := item.senderState.Balance
	currentTransactionFee := item.currentTransaction.Fee.Load()
	futureAccumulatedFee := new(big.Int).Add(item.accumulatedFee, currentTransactionFee)

	willFeeExceedBalance := futureAccumulatedFee.Cmp(senderBalance) > 0
	if willFeeExceedBalance {
		logSelect.Trace("transactionsHeapItem.detectWillFeeExceedBalance",
			"tx", item.currentTransaction.TxHash,
			"sender", item.sender,
			"balance", item.senderState.Balance,
			"accumulatedFee", item.accumulatedFee,
		)
	}

	return willFeeExceedBalance
}

func (item *transactionsHeapItem) detectLowerNonce() bool {
	if !item.senderStateProvided {
		// This should never happen during selection.
		return false
	}

	isLowerNonce := item.currentTransactionNonce < item.senderState.Nonce
	if isLowerNonce {
		logSelect.Trace("transactionsHeapItem.detectLowerNonce",
			"tx", item.currentTransaction.TxHash,
			"nonce", item.currentTransactionNonce,
			"sender", item.sender,
			"senderState.Nonce", item.senderState.Nonce,
		)
	}

	return isLowerNonce
}

func (item *transactionsHeapItem) detectBadlyGuarded() bool {
	if !item.senderStateProvided {
		// This should never happen during selection.
		return false
	}

	transactionGuardian := *item.currentTransaction.Guardian.Load()
	accountGuardian := item.senderState.Guardian
	isBadlyGuarded := bytes.Compare(transactionGuardian, accountGuardian) != 0
	if isBadlyGuarded {
		logSelect.Trace("transactionsHeapItem.detectBadlyGuarded",
			"tx", item.currentTransaction.TxHash,
			"sender", item.sender,
			"transactionGuardian", transactionGuardian,
			"accountGuardian", accountGuardian,
		)
	}

	return isBadlyGuarded
}

func (item *transactionsHeapItem) detectNonceDuplicate() bool {
	if item.latestSelectedTransaction == nil {
		return false
	}
	if !item.senderStateProvided {
		// This should never happen during selection.
		return false
	}

	isDuplicate := item.currentTransactionNonce == item.latestSelectedTransactionNonce
	if isDuplicate {
		logSelect.Trace("transactionsHeapItem.detectNonceDuplicate",
			"tx", item.currentTransaction.TxHash,
			"sender", item.sender,
			"nonce", item.currentTransactionNonce,
		)
	}

	return isDuplicate
}

func (item *transactionsHeapItem) requestAccountStateIfNecessary(accountStateProvider AccountStateProvider) {
	if item.senderStateRequested {
		return
	}

	item.senderStateRequested = true
	senderState, err := accountStateProvider.GetAccountState(item.sender)
	if err != nil {
		// Hazardous; should never happen.
		logSelect.Debug("transactionsHeapItem.requestAccountStateIfNecessary: nonce not available", "sender", item.sender, "err", err)
		return
	}

	item.senderStateProvided = true
	item.senderState = senderState
}