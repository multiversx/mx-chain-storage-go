package immunitycache

import (
	"container/list"
	"sync"

	"github.com/multiversx/mx-chain-core-go/core"
	"github.com/multiversx/mx-chain-core-go/core/atomic"

	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/types"
)

// immunityChunk owns a shard of the cache's keyspace.
//
// Immune intent invariants (held under mutex.Lock):
//   - immuneKeys[k] == n           iff   nonceToKeys[n][k] is set.
//   - maxImmuneNonce == max(keys of nonceToKeys)   (0 when empty).
//   - sum of len(immuneKeys) across all chunks == globalImmuneCounter.Get().
type immunityChunk struct {
	config              immunityChunkConfig
	items               map[string]chunkItemWrapper
	itemsAsList         *list.List
	immuneKeys          map[string]uint64
	nonceToKeys         map[uint64]map[string]struct{}
	maxImmuneNonce      uint64
	oldestImmuneNonce   uint64
	numBytes            int
	mutex               sync.RWMutex
	globalImmuneCounter *atomic.Counter
}

type chunkItemWrapper struct {
	item        *cacheItem
	listElement *list.Element
}

// newImmunityChunk creates a chunk. The caller must pass a non-nil counter
// (shared across the cache's chunks of the same generation; see ImmunityCache.Clear).
func newImmunityChunk(config immunityChunkConfig, globalImmuneCounter *atomic.Counter) *immunityChunk {
	log.Trace("newImmunityChunk", "config", config.String())

	return &immunityChunk{
		config:              config,
		items:               make(map[string]chunkItemWrapper),
		itemsAsList:         list.New(),
		immuneKeys:          make(map[string]uint64),
		nonceToKeys:         make(map[uint64]map[string]struct{}),
		globalImmuneCounter: globalImmuneCounter,
	}
}

// ImmunizeKeys marks keys as immune for the given confirmation nonce.
// At intent capacity, a NEW key is accepted only when nonce < maxImmuneNonce,
// displacing one intent at maxImmuneNonce (and evicting its in-cache item, if any).
// Existing keys are upgraded to max(oldNonce, nonce); downgrades and no-ops are skipped.
func (chunk *immunityChunk) ImmunizeKeys(keys [][]byte, nonce uint64) (numNow, numFuture int) {
	chunk.mutex.Lock()
	defer chunk.mutex.Unlock()

	if nonce < chunk.oldestImmuneNonce {
		return
	}

	capacity := uint64(chunk.config.maxNumItems)

	for _, key := range keys {
		keyStr := string(key)
		oldNonce, exists := chunk.immuneKeys[keyStr]

		if exists && oldNonce >= nonce {
			continue
		}

		if !exists && uint64(len(chunk.immuneKeys)) >= capacity {
			if chunk.maxImmuneNonce <= nonce {
				continue
			}
			chunk.displaceOneIntentAtMaxNonceNoLock()
		}

		if exists {
			chunk.removeImmuneKeyNoLock(keyStr)
		}
		chunk.addImmuneKeyNoLock(keyStr, nonce)

		if item, ok := chunk.getItemNoLock(keyStr); ok {
			item.setImmuneNonce(nonce)
			numNow++
		} else {
			numFuture++
		}
	}

	return
}

// SetOldestImmuneNonce raises the de-immunization frontier. Intents below the
// new value are dropped; items with immuneNonce below it become evictable.
func (chunk *immunityChunk) SetOldestImmuneNonce(nonce uint64) {
	chunk.mutex.Lock()
	defer chunk.mutex.Unlock()

	if nonce > chunk.oldestImmuneNonce {
		chunk.oldestImmuneNonce = nonce
	}

	// TODO investigate if it is more efficient to trigger the cleanup only when the map si full
	chunk.cleanupInactiveImmuneKeysNoLock()
}

func (chunk *immunityChunk) getItemNoLock(key string) (*cacheItem, bool) {
	wrapper, ok := chunk.items[key]
	if !ok {
		return nil, false
	}

	return wrapper.item, true
}

// AddItem add an item to the chunk
func (chunk *immunityChunk) AddItem(item *cacheItem) (has, added bool) {
	chunk.mutex.Lock()
	defer chunk.mutex.Unlock()

	if chunk.itemExistsNoLock(item) {
		return true, false
	}

	chunk.immunizeItemOnAddNoLock(item)
	err := chunk.evictItemsIfCapacityExceededNoLock(item)
	if err != nil {
		return false, false
	}

	chunk.addItemNoLock(item)
	chunk.trackNumBytesOnAddNoLock(item)
	return false, true
}

func (chunk *immunityChunk) evictItemsIfCapacityExceededNoLock(incomingItem *cacheItem) error {
	if !chunk.isCapacityExceededNoLock() {
		return nil
	}

	numRemoved, err := chunk.evictItemsNoLock()
	if err == nil {
		chunk.monitorEvictionNoLock(numRemoved, nil)
		return nil
	}
	if !incomingItem.isImmuneToEviction(chunk.oldestImmuneNonce) {
		chunk.monitorEvictionNoLock(numRemoved, err)
		return err
	}

	// All in-cache items are immune. We may displace those with nonce
	// strictly greater than the incoming item's nonce (farther future first).
	for chunk.isCapacityExceededNoLock() {
		if !chunk.removeHighestImmuneInCacheNoLock(incomingItem.nonce) {
			chunk.monitorEvictionNoLock(numRemoved, err)
			return err
		}
		numRemoved++
	}

	// We successfully evicted enough items to fit the incoming immune item. Monitor the eviction (if any) and proceed
	// with the addition so no error is returned to the caller.
	chunk.monitorEvictionNoLock(numRemoved, nil)
	return nil
}

func (chunk *immunityChunk) isCapacityExceededNoLock() bool {
	tooManyItems := uint64(len(chunk.items)) >= uint64(chunk.config.maxNumItems)
	tooManyBytes := uint64(chunk.numBytes) >= uint64(chunk.config.maxNumBytes)
	return tooManyItems || tooManyBytes
}

func (chunk *immunityChunk) evictItemsNoLock() (numRemoved int, err error) {
	numToRemoveEachStep := int(chunk.config.numItemsToPreemptivelyEvict)

	// We perform the first step out of the loop in order to detect & return error
	numRemovedInStep := chunk.removeOldestNoLock(numToRemoveEachStep)
	numRemoved += numRemovedInStep

	if numRemovedInStep == 0 {
		return 0, common.ErrFailedCacheEviction
	}

	for chunk.isCapacityExceededNoLock() && numRemovedInStep == numToRemoveEachStep {
		numRemovedInStep = chunk.removeOldestNoLock(numToRemoveEachStep)
		numRemoved += numRemovedInStep
	}

	return numRemoved, nil
}

func (chunk *immunityChunk) removeOldestNoLock(numToRemove int) int {
	numRemoved := 0
	element := chunk.itemsAsList.Front()

	for element != nil && numRemoved < numToRemove {
		item := element.Value.(*cacheItem)

		if item.isImmuneToEviction(chunk.oldestImmuneNonce) {
			element = element.Next()
			continue
		}

		elementToRemove := element
		element = element.Next()

		chunk.removeNoLock(elementToRemove)
		numRemoved++
	}

	return numRemoved
}

// removeNoLock removes an in-cache item; if it had an immune intent the intent
// is dropped too (which decrements the shared counter and updates the indices).
func (chunk *immunityChunk) removeNoLock(element *list.Element) {
	item := element.Value.(*cacheItem)
	delete(chunk.items, item.key)
	chunk.itemsAsList.Remove(element)
	chunk.trackNumBytesOnRemoveNoLock(item)
	chunk.removeImmuneKeyNoLock(item.key)
}

func (chunk *immunityChunk) monitorEvictionNoLock(numRemoved int, err error) {
	if err != nil {
		log.Trace("immunityChunk.monitorEviction()", "name", chunk.config.cacheName, "numRemoved", numRemoved, "err", err)
	}
}

func (chunk *immunityChunk) itemExistsNoLock(item *cacheItem) bool {
	_, exists := chunk.items[item.key]
	return exists
}

// First, we insert (append) in the linked list; then in the map.
// In the map, we also need to hold a reference to the list element, to have O(1) removal.
func (chunk *immunityChunk) addItemNoLock(item *cacheItem) {
	element := chunk.itemsAsList.PushBack(item)
	chunk.items[item.key] = chunkItemWrapper{item: item, listElement: element}
}

func (chunk *immunityChunk) immunizeItemOnAddNoLock(item *cacheItem) {
	immuneNonce, immunize := chunk.immuneKeys[item.key]
	if immunize {
		item.setImmuneNonce(immuneNonce)
	}
}

func (chunk *immunityChunk) trackNumBytesOnAddNoLock(item *cacheItem) {
	chunk.numBytes += item.size
}

// GetItem gets an item from the chunk
func (chunk *immunityChunk) GetItem(key string) (*cacheItem, bool) {
	chunk.mutex.RLock()
	defer chunk.mutex.RUnlock()
	return chunk.getItemNoLock(key)
}

// RemoveItem removes an item from the chunk and clears any pending future immune intent.
// Removing an unknown key clears its future intent (useful for rolling back optimistic immunization).
func (chunk *immunityChunk) RemoveItem(key string) bool {
	chunk.mutex.Lock()
	defer chunk.mutex.Unlock()

	wrapper, ok := chunk.items[key]
	if !ok {
		chunk.removeImmuneKeyNoLock(key)
		return false
	}

	chunk.removeNoLock(wrapper.listElement)
	return true
}

func (chunk *immunityChunk) trackNumBytesOnRemoveNoLock(item *cacheItem) {
	chunk.numBytes -= item.size
	chunk.numBytes = core.MaxInt(chunk.numBytes, 0)
}

// RemoveOldest removes a number of old items
func (chunk *immunityChunk) RemoveOldest(numToRemove int) int {
	chunk.mutex.Lock()
	defer chunk.mutex.Unlock()
	return chunk.removeOldestNoLock(numToRemove)
}

// Count counts the items
func (chunk *immunityChunk) Count() int {
	chunk.mutex.RLock()
	defer chunk.mutex.RUnlock()
	return len(chunk.items)
}

// CountImmune counts the immune intents tracked by this chunk.
func (chunk *immunityChunk) CountImmune() int {
	chunk.mutex.RLock()
	defer chunk.mutex.RUnlock()
	return len(chunk.immuneKeys)
}

// NumBytes gets the number of bytes stored
func (chunk *immunityChunk) NumBytes() int {
	chunk.mutex.RLock()
	defer chunk.mutex.RUnlock()
	return chunk.numBytes
}

// KeysInOrder gets the keys, in order
func (chunk *immunityChunk) KeysInOrder() [][]byte {
	chunk.mutex.RLock()
	defer chunk.mutex.RUnlock()

	keys := make([][]byte, 0, chunk.itemsAsList.Len())
	for element := chunk.itemsAsList.Front(); element != nil; element = element.Next() {
		item := element.Value.(*cacheItem)
		keys = append(keys, []byte(item.key))
	}

	return keys
}

// AppendKeys accumulates keys in a given slice
func (chunk *immunityChunk) AppendKeys(keysAccumulator [][]byte) [][]byte {
	chunk.mutex.RLock()
	defer chunk.mutex.RUnlock()

	for key := range chunk.items {
		keysAccumulator = append(keysAccumulator, []byte(key))
	}

	return keysAccumulator
}

// ForEachItem iterates over the items in the chunk
func (chunk *immunityChunk) ForEachItem(function types.ForEachItem) {
	chunk.mutex.RLock()
	defer chunk.mutex.RUnlock()

	for key, itemWrapper := range chunk.items {
		function([]byte(key), itemWrapper.item.payload)
	}
}

// IsInterfaceNil returns true if there is no value under the interface
func (chunk *immunityChunk) IsInterfaceNil() bool {
	return chunk == nil
}

// addImmuneKeyNoLock registers a NEW immune intent for `key` at `nonce`.
// Caller must ensure the key isn't already tracked (call removeImmuneKeyNoLock first
// to upgrade an existing intent's nonce, preserving bucket invariants).
func (chunk *immunityChunk) addImmuneKeyNoLock(key string, nonce uint64) {
	chunk.immuneKeys[key] = nonce

	bucket, ok := chunk.nonceToKeys[nonce]
	if !ok {
		bucket = make(map[string]struct{})
		chunk.nonceToKeys[nonce] = bucket
	}
	bucket[key] = struct{}{}

	if nonce > chunk.maxImmuneNonce {
		chunk.maxImmuneNonce = nonce
	}
	chunk.globalImmuneCounter.Increment()
}

// removeImmuneKeyNoLock drops the immune intent for `key`, if any.
// Recomputes maxImmuneNonce when the last key at the current max leaves.
func (chunk *immunityChunk) removeImmuneKeyNoLock(key string) {
	oldNonce, ok := chunk.immuneKeys[key]
	if !ok {
		return
	}
	delete(chunk.immuneKeys, key)

	bucket := chunk.nonceToKeys[oldNonce]
	delete(bucket, key)
	if len(bucket) == 0 {
		delete(chunk.nonceToKeys, oldNonce)
		if oldNonce == chunk.maxImmuneNonce {
			chunk.recomputeMaxImmuneNonceNoLock()
		}
	}

	chunk.globalImmuneCounter.Decrement()
}

func (chunk *immunityChunk) recomputeMaxImmuneNonceNoLock() {
	var maxN uint64
	for n := range chunk.nonceToKeys {
		if n > maxN {
			maxN = n
		}
	}
	chunk.maxImmuneNonce = maxN
}

// displaceOneIntentAtMaxNonceNoLock drops one immune intent at maxImmuneNonce.
// Prefers evicting an in-cache item (which frees an items slot); otherwise drops
// a future-only intent. Caller must ensure maxImmuneNonce points to a non-empty bucket.
func (chunk *immunityChunk) displaceOneIntentAtMaxNonceNoLock() {
	bucket := chunk.nonceToKeys[chunk.maxImmuneNonce]

	for k := range bucket {
		if wrapper, ok := chunk.items[k]; ok {
			chunk.removeNoLock(wrapper.listElement)
			return
		}
	}

	for k := range bucket {
		chunk.removeImmuneKeyNoLock(k)
		return
	}
}

// removeHighestImmuneInCacheNoLock removes ONE in-cache immune item whose
// immuneNonce is strictly greater than `threshold`. Returns true on success.
// Walks `nonceToKeys` once (O(B * average bucket size), B = distinct nonces).
func (chunk *immunityChunk) removeHighestImmuneInCacheNoLock(threshold uint64) bool {
	var bestNonce uint64
	var bestKey string
	found := false

	for n, bucket := range chunk.nonceToKeys {
		if n <= threshold {
			continue
		}
		if found && n <= bestNonce {
			continue
		}
		for k := range bucket {
			if _, inCache := chunk.items[k]; inCache {
				bestNonce = n
				bestKey = k
				found = true
				break
			}
		}
	}

	if !found {
		return false
	}

	wrapper := chunk.items[bestKey]
	chunk.removeNoLock(wrapper.listElement)
	return true
}

// cleanupInactiveImmuneKeysNoLock drops every intent whose nonce is below
// chunk.oldestImmuneNonce. O(B + dropped) where B = distinct nonces.
func (chunk *immunityChunk) cleanupInactiveImmuneKeysNoLock() {
	needsRecompute := false
	for n, bucket := range chunk.nonceToKeys {
		if n >= chunk.oldestImmuneNonce {
			continue
		}
		for k := range bucket {
			delete(chunk.immuneKeys, k)
			chunk.globalImmuneCounter.Decrement()
		}
		delete(chunk.nonceToKeys, n)
		if n == chunk.maxImmuneNonce {
			needsRecompute = true
		}
	}
	if needsRecompute {
		chunk.recomputeMaxImmuneNonceNoLock()
	}
}
