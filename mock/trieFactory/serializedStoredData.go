package trieFactory

// SerializedStoredDataStub -
type SerializedStoredDataStub struct {
	GetSerializedCalled func() []byte
	SetSerializedCalled func(bytes []byte)
}

// GetSerialized -
func (s *SerializedStoredDataStub) GetSerialized() []byte {
	if s.GetSerializedCalled != nil {
		return s.GetSerializedCalled()
	}

	return make([]byte, 0)
}

// SetSerialized -
func (s *SerializedStoredDataStub) SetSerialized(bytes []byte) {
	if s.SetSerializedCalled != nil {
		s.SetSerializedCalled(bytes)
	}
}
