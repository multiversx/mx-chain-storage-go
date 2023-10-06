package testscommon

// EvictionNotifierStub -
type EvictionNotifierStub struct {
	NotifyEvictionCalled       func(txHash []byte, cacheId string)
	ShouldNotifyEvictionCalled func(txHash []byte) bool
}

// NotifyEviction -
func (stub *EvictionNotifierStub) NotifyEviction(txHash []byte, cacheId string) {
	if stub.NotifyEvictionCalled != nil {
		stub.NotifyEvictionCalled(txHash, cacheId)
	}
}

// ShouldNotifyEviction -
func (stub *EvictionNotifierStub) ShouldNotifyEviction(txHash []byte) bool {
	if stub.ShouldNotifyEvictionCalled != nil {
		return stub.ShouldNotifyEvictionCalled(txHash)
	}
	return false
}

// IsInterfaceNil -
func (stub *EvictionNotifierStub) IsInterfaceNil() bool {
	return stub == nil
}
