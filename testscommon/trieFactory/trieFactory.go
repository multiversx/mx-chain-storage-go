package trieFactory

type trieNodeFactory struct {
}

// NewTrieNodeFactory creates a new trieNodeFactory
func NewTrieNodeFactory() *trieNodeFactory {
	return &trieNodeFactory{}
}

// CreateEmpty returns an empty InterceptedTrieNode
func (tnf *trieNodeFactory) CreateEmpty() interface{} {
	return &SerializedStoredDataStub{}
}

// IsInterfaceNil returns true if there is no value under the interface
func (tnf *trieNodeFactory) IsInterfaceNil() bool {
	return tnf == nil
}
