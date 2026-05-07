package immunitycache

type cacheItem struct {
	payload     interface{}
	key         string
	size        int
	immuneNonce uint64
}

func newCacheItem(payload interface{}, key string, size int) *cacheItem {
	return &cacheItem{
		payload: payload,
		key:     key,
		size:    size,
	}
}

func (item *cacheItem) isImmuneToEviction(oldestImmuneNonce uint64) bool {
	return item.immuneNonce >= oldestImmuneNonce && item.immuneNonce > 0
}

func (item *cacheItem) setImmuneNonce(nonce uint64) {
	if nonce > item.immuneNonce {
		item.immuneNonce = nonce
	}
}
