package common

// CacheConfig will map the cache configuration
type CacheConfig struct {
	Name                 string
	Type                 string
	Capacity             uint32
	SizePerSender        uint32
	SizeInBytes          uint64
	SizeInBytesPerSender uint32
	Shards               uint32
}

// DBConfig will map the database configuration
type DBConfig struct {
	FilePath          string
	Type              string
	BatchDelaySeconds int
	MaxBatchSize      int
	MaxOpenFiles      int
	UseTmpAsFilePath  bool
}
