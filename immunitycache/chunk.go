package immunitycache

import (
	"container/list"
	"sync"

	"github.com/multiversx/mx-chain-core-go/core"
	"github.com/multiversx/mx-chain-storage-go/common"
	"github.com/multiversx/mx-chain-storage-go/types"
)

var emptyStruct struct{}

type immunityChunk struct {
	config            immunityChunkConfig
	items             map[string]chunkItemWrapper
	itemsAsList       *list.List
	immuneKeys        map[string]uint64
	oldestImmuneNonce uint64
	numBytes          int
	mutex             sync.RWMutex
}

type chunkItemWrapper struct {
	item        *cacheItem
	listElement *list.Element
}

func newImmunityChunk(config immunityChunkConfig) *immunityChunk {
	log.Trace("newImmunityChunk", "config", config.String())

	return &immunityChunk{
		config:      config,
		items:       make(map[string]chunkItemWrapper),
		itemsAsList: list.New(),
		immuneKeys:  make(map[string]uint64),
	}
}

// ImmunizeKeys marks keys as immune to eviction
func (chunk *immunityChunk) ImmunizeKeys(keys [][]byte, nonce uint64) (numNow, numFuture int) {
	chunk.mutex.Lock()
	defer chunk.mutex.Unlock()

	for _, key := range keys {
		keyAsString := string(key)
		item, ok := chunk.getItemNoLock(keyAsString)

		if ok {
			item.setImmuneNonce(nonce)
			numNow++
		} else {
			numFuture++
		}

		storedNonce := chunk.immuneKeys[keyAsString]
		if nonce > storedNonce {
			chunk.immuneKeys[keyAsString] = nonce
		}
	}

	return
}

func (chunk *immunityChunk) SetOldestImmuneNonce(nonce uint64) {
	chunk.mutex.Lock()
	defer chunk.mutex.Unlock()

	if nonce > chunk.oldestImmuneNonce {
		chunk.oldestImmuneNonce = nonce
	}

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

	// Discard duplicates
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
		chunk.monitorEvictionNoLock(numRemoved, err)
		return nil
	}
	if !incomingItem.isImmuneToEviction(chunk.oldestImmuneNonce) {
		chunk.monitorEvictionNoLock(numRemoved, err)
		return err
	}

	for chunk.isCapacityExceededNoLock() {
		if !chunk.removeFarthestImmuneNoLock(incomingItem.immuneNonce) {
			chunk.monitorEvictionNoLock(numRemoved, err)
			return err
		}
		numRemoved++
	}

	chunk.monitorEvictionNoLock(numRemoved, nil)
	return nil
}

func (chunk *immunityChunk) isCapacityExceededNoLock() bool {
	tooManyItems := len(chunk.items) >= int(chunk.config.maxNumItems)
	tooManyBytes := chunk.numBytes >= int(chunk.config.maxNumBytes)
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

func (chunk *immunityChunk) removeNoLock(element *list.Element) {
	item := element.Value.(*cacheItem)
	delete(chunk.items, item.key)
	delete(chunk.immuneKeys, item.key)
	chunk.itemsAsList.Remove(element)
	chunk.trackNumBytesOnRemoveNoLock(item)
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

// RemoveItem removes an item from the chunk
// In order to improve the robustness of the cache, we'll also remove from "keysToImmunizeFuture",
// even if the item does not actually exist in the cache - to allow un-doing immunization intent (perhaps useful for rollbacks).
func (chunk *immunityChunk) RemoveItem(key string) bool {
	chunk.mutex.Lock()
	defer chunk.mutex.Unlock()

	delete(chunk.immuneKeys, key)

	wrapper, ok := chunk.items[key]
	if !ok {
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

// CountImmune counts the immune items
func (chunk *immunityChunk) CountImmune() int {
	chunk.mutex.RLock()
	defer chunk.mutex.RUnlock()

	count := 0
	for key, immuneNonce := range chunk.immuneKeys {
		if immuneNonce < chunk.oldestImmuneNonce {
			continue
		}

		wrapper, ok := chunk.items[key]
		if !ok {
			count++
			continue
		}
		if wrapper.item.isImmuneToEviction(chunk.oldestImmuneNonce) {
			count++
		}
	}

	return count
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

func (chunk *immunityChunk) removeFarthestImmuneNoLock(referenceNonce uint64) bool {
	var selectedElement *list.Element
	var maxDistance uint64

	for element := chunk.itemsAsList.Front(); element != nil; element = element.Next() {
		item := element.Value.(*cacheItem)
		if !item.isImmuneToEviction(chunk.oldestImmuneNonce) {
			continue
		}

		distance := computeNonceDistance(referenceNonce, item.immuneNonce)
		if selectedElement == nil || distance > maxDistance {
			selectedElement = element
			maxDistance = distance
		}
	}

	if selectedElement == nil {
		return false
	}

	chunk.removeNoLock(selectedElement)
	return true
}

func (chunk *immunityChunk) cleanupInactiveImmuneKeysNoLock() {
	for key, immuneNonce := range chunk.immuneKeys {
		if immuneNonce >= chunk.oldestImmuneNonce {
			continue
		}

		wrapper, ok := chunk.items[key]
		if ok && wrapper.item.isImmuneToEviction(chunk.oldestImmuneNonce) {
			continue
		}

		delete(chunk.immuneKeys, key)
	}
}

func computeNonceDistance(firstNonce uint64, secondNonce uint64) uint64 {
	if firstNonce >= secondNonce {
		return firstNonce - secondNonce
	}

	return secondNonce - firstNonce
}
