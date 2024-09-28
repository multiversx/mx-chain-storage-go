package leveldb

// PutBatch will call the unexported putBatch function
func (s *SerialDB) PutBatch() {
	_ = s.putBatch()
}
