package leveldb

import (
	"sync"
)

type mapKeysRemovedHandler struct {
	mut            sync.RWMutex
	removedByBatch map[uint64]map[string]struct{}
}

func newMapKeysRemovedHandler() *mapKeysRemovedHandler {
	return &mapKeysRemovedHandler{
		removedByBatch: make(map[uint64]map[string]struct{}),
	}
}

// addRemovedKeys will add a map containing removed keys for a specified batch ID
func (handler *mapKeysRemovedHandler) addRemovedKeys(keys map[string]struct{}, batchID uint64) {
	handler.mut.Lock()
	defer handler.mut.Unlock()

	if len(keys) == 0 {
		// no need to add an empty map here
		return
	}

	handler.removedByBatch[batchID] = keys
}

// deleteRemovedKeys will delete the map containing removed keys for a specified batch ID
func (handler *mapKeysRemovedHandler) deleteRemovedKeys(batchID uint64) {
	handler.mut.Lock()
	defer handler.mut.Unlock()

	delete(handler.removedByBatch, batchID)
}

// hasRemovedKeys will return true if the key is contained in any of the inner maps
func (handler *mapKeysRemovedHandler) hasRemovedKeys(key []byte) bool {
	handler.mut.RLock()
	defer handler.mut.RUnlock()

	for _, m := range handler.removedByBatch {
		_, found := m[string(key)]
		if found {
			return true
		}
	}

	return false
}
