package pebbledb

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"

	"github.com/multiversx/mx-chain-storage-go/common"
)

const (
	// DefaultProfile suits units with a small, fixed per-block write volume
	DefaultProfile = "default"
	// HeavyWriteProfile suits units whose write volume scales with the transaction load
	HeavyWriteProfile = "heavy-write"

	megabyte        = 1 << 20
	walBytesPerSync = 512 << 10
	minMaxOpenFiles = 1000
	// formatMajorVersion is pinned on purpose: a newer on-disk format cannot be opened by older binaries
	formatMajorVersion = pebble.FormatTableFormatV6
	// memTableStopWritesThreshold bounds the memtables alive at once, so memory per writing instance
	// stays within memTableSize x this value
	memTableStopWritesThreshold = 2
)

type profile struct {
	memTableSize             uint64
	l0CompactionThreshold    int
	l0StopWritesThreshold    int
	maxConcurrentCompactions int
}

var profiles = map[string]profile{
	DefaultProfile: {
		memTableSize:             8 * megabyte,
		l0CompactionThreshold:    4,
		l0StopWritesThreshold:    12,
		maxConcurrentCompactions: 1,
	},
	HeavyWriteProfile: {
		memTableSize:             32 * megabyte,
		l0CompactionThreshold:    2,
		l0StopWritesThreshold:    24,
		maxConcurrentCompactions: 4,
	},
}

func buildOptions(args ArgsNewDB) (*pebble.Options, error) {
	profileName := args.Profile
	if len(profileName) == 0 {
		profileName = DefaultProfile
	}
	p, found := profiles[profileName]
	if !found {
		return nil, fmt.Errorf("%w: unknown pebble profile %s", common.ErrInvalidConfig, profileName)
	}

	opts := &pebble.Options{
		MemTableSize:                p.memTableSize,
		MemTableStopWritesThreshold: memTableStopWritesThreshold,
		L0CompactionThreshold:       p.l0CompactionThreshold,
		L0StopWritesThreshold:       p.l0StopWritesThreshold,
		CompactionConcurrencyRange: func() (int, int) {
			return 1, p.maxConcurrentCompactions
		},
		WALBytesPerSync:    walBytesPerSync,
		FormatMajorVersion: formatMajorVersion,
		MaxOpenFiles:       max(args.MaxOpenFiles, minMaxOpenFiles),
		Logger:             pebbleLogger{},
	}

	if args.BloomFilterBitsPerKey > 0 {
		policy := bloom.FilterPolicy(args.BloomFilterBitsPerKey)
		for i := range opts.Levels {
			opts.Levels[i].FilterPolicy = policy
		}
	}

	if args.SharedResources != nil {
		opts.Cache = args.SharedResources.cacheHolder.Cache
		opts.FileCache = args.SharedResources.fileCache
	}

	return opts, nil
}
