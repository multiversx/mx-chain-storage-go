package txcache

import (
	"encoding/json"
	"fmt"

	"github.com/multiversx/mx-chain-storage-go/common"
)

const numChunksLowerBound = 1
const numChunksUpperBound = 128
const maxNumItemsLowerBound = 4
const maxNumBytesLowerBound = maxNumItemsLowerBound * 1
const maxNumBytesUpperBound = 1_073_741_824 // one GB
const maxNumItemsPerSenderLowerBound = 1
const maxNumBytesPerSenderLowerBound = maxNumItemsPerSenderLowerBound * 1
const maxNumBytesPerSenderUpperBound = 33_554_432 // 32 MB
const numItemsToPreemptivelyEvictLowerBound = uint32(1)

// ConfigSourceMe holds cache configuration
type ConfigSourceMe struct {
	Name                        string
	NumChunks                   uint32
	EvictionEnabled             bool
	NumBytesThreshold           uint32
	NumBytesPerSenderThreshold  uint32
	CountThreshold              uint32
	CountPerSenderThreshold     uint32
	NumItemsToPreemptivelyEvict uint32
}

type senderConstraints struct {
	maxNumTxs   uint32
	maxNumBytes uint32
}

func (config *ConfigSourceMe) verify() error {
	if len(config.Name) == 0 {
		return fmt.Errorf("%w: config.Name is invalid", common.ErrInvalidConfig)
	}
	if config.NumChunks < numChunksLowerBound || config.NumChunks > numChunksUpperBound {
		return fmt.Errorf("%w: config.NumChunks is invalid", common.ErrInvalidConfig)
	}
	if config.NumBytesPerSenderThreshold < maxNumBytesPerSenderLowerBound || config.NumBytesPerSenderThreshold > maxNumBytesPerSenderUpperBound {
		return fmt.Errorf("%w: config.NumBytesPerSenderThreshold is invalid", common.ErrInvalidConfig)
	}
	if config.CountPerSenderThreshold < maxNumItemsPerSenderLowerBound {
		return fmt.Errorf("%w: config.CountPerSenderThreshold is invalid", common.ErrInvalidConfig)
	}

	if config.NumBytesThreshold < maxNumBytesLowerBound || config.NumBytesThreshold > maxNumBytesUpperBound {
		return fmt.Errorf("%w: config.NumBytesThreshold is invalid", common.ErrInvalidConfig)
	}
	if config.CountThreshold < maxNumItemsLowerBound {
		return fmt.Errorf("%w: config.CountThreshold is invalid", common.ErrInvalidConfig)
	}
	if config.NumItemsToPreemptivelyEvict < numItemsToPreemptivelyEvictLowerBound {
		return fmt.Errorf("%w: config.NumItemsToPreemptivelyEvict is invalid", common.ErrInvalidConfig)
	}

	return nil
}

func (config *ConfigSourceMe) getSenderConstraints() senderConstraints {
	return senderConstraints{
		maxNumBytes: config.NumBytesPerSenderThreshold,
		maxNumTxs:   config.CountPerSenderThreshold,
	}
}

// String returns a readable representation of the object
func (config *ConfigSourceMe) String() string {
	bytes, err := json.Marshal(config)
	if err != nil {
		log.Error("ConfigSourceMe.String", "err", err)
	}

	return string(bytes)
}

// ConfigDestinationMe holds cache configuration
type ConfigDestinationMe struct {
	Name                        string
	NumChunks                   uint32
	MaxNumItems                 uint32
	MaxNumBytes                 uint32
	NumItemsToPreemptivelyEvict uint32
}

func (config *ConfigDestinationMe) verify() error {
	if len(config.Name) == 0 {
		return fmt.Errorf("%w: config.Name is invalid", common.ErrInvalidConfig)
	}
	if config.NumChunks < numChunksLowerBound || config.NumChunks > numChunksUpperBound {
		return fmt.Errorf("%w: config.NumChunks is invalid", common.ErrInvalidConfig)
	}
	if config.MaxNumItems < maxNumItemsLowerBound {
		return fmt.Errorf("%w: config.MaxNumItems is invalid", common.ErrInvalidConfig)
	}
	if config.MaxNumBytes < maxNumBytesLowerBound || config.MaxNumBytes > maxNumBytesUpperBound {
		return fmt.Errorf("%w: config.MaxNumBytes is invalid", common.ErrInvalidConfig)
	}
	if config.NumItemsToPreemptivelyEvict < numItemsToPreemptivelyEvictLowerBound {
		return fmt.Errorf("%w: config.NumItemsToPreemptivelyEvict is invalid", common.ErrInvalidConfig)
	}

	return nil
}

// String returns a readable representation of the object
func (config *ConfigDestinationMe) String() string {
	bytes, err := json.Marshal(config)
	if err != nil {
		log.Error("ConfigDestinationMe.String", "err", err)
	}

	return string(bytes)
}
