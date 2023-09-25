package testscommon

// EvictionNotifierStub -
type EvictionNotifierStub struct {
	NotifyEvictionCalled func(txHash []byte)
}

// NotifyEviction -
func (stub *EvictionNotifierStub) NotifyEviction(txHash []byte) {
	if stub.NotifyEvictionCalled != nil {
		stub.NotifyEvictionCalled(txHash)
	}
}

// IsInterfaceNil -
func (stub *EvictionNotifierStub) IsInterfaceNil() bool {
	return stub == nil
}
