package testscommon

import (
	"sync"

	"github.com/multiversx/mx-chain-storage-go/types"
)

type cacherMock struct {
	mut  sync.RWMutex
	data map[string]interface{}
}

// NewCacherMock -
func NewCacherMock() *cacherMock {
	return &cacherMock{
		data: make(map[string]interface{}),
	}
}

// Clear -
func (mock *cacherMock) Clear() {
	mock.mut.Lock()
	defer mock.mut.Unlock()

	mock.data = make(map[string]interface{})
}

// Put -
func (mock *cacherMock) Put(key []byte, value interface{}, _ int) (evicted bool) {
	mock.mut.Lock()
	defer mock.mut.Unlock()

	mock.data[string(key)] = value

	return false
}

// Get -
func (mock *cacherMock) Get(key []byte) (value interface{}, ok bool) {
	mock.mut.RLock()
	defer mock.mut.RUnlock()

	val, found := mock.data[string(key)]

	return val, found
}

// Has -
func (mock *cacherMock) Has(key []byte) bool {
	mock.mut.RLock()
	defer mock.mut.RUnlock()

	_, found := mock.data[string(key)]

	return found
}

// Peek -
func (mock *cacherMock) Peek(key []byte) (value interface{}, ok bool) {
	return mock.Get(key)
}

// HasOrAdd -
func (mock *cacherMock) HasOrAdd(key []byte, value interface{}, _ int) (has, added bool) {
	mock.mut.Lock()
	defer mock.mut.Unlock()

	_, found := mock.data[string(key)]
	if found {
		return found, !found
	}

	mock.data[string(key)] = value

	return found, !found
}

// Remove -
func (mock *cacherMock) Remove(key []byte) {
	mock.mut.Lock()
	defer mock.mut.Unlock()

	delete(mock.data, string(key))
}

// Keys -
func (mock *cacherMock) Keys() [][]byte {
	mock.mut.RLock()
	defer mock.mut.RUnlock()

	keys := make([][]byte, 0, len(mock.data))
	for key := range mock.data {
		keys = append(keys, []byte(key))
	}

	return keys
}

// Len -
func (mock *cacherMock) Len() int {
	mock.mut.RLock()
	defer mock.mut.RUnlock()

	return len(mock.data)
}

// SizeInBytesContained -
func (mock *cacherMock) SizeInBytesContained() uint64 {
	return 0
}

// MaxSize -
func (mock *cacherMock) MaxSize() int {
	return 0
}

// RegisterHandler -
func (mock *cacherMock) RegisterHandler(_ func(key []byte, value interface{}), _ string) {
}

// UnRegisterHandler -
func (mock *cacherMock) UnRegisterHandler(_ string) {
}

// GetRemovalStatus -
func (mock *cacherMock) GetRemovalStatus(_ []byte) types.RemovalStatus {
	return types.UnknownRemovalStatus
}

// Close -
func (mock *cacherMock) Close() error {
	return nil
}

// IsInterfaceNil -
func (mock *cacherMock) IsInterfaceNil() bool {
	return mock == nil
}
