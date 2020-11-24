package pruning

import (
	"encoding/hex"
	"fmt"
	"math"

	"github.com/ElrondNetwork/elrond-go/storage"
	"github.com/ElrondNetwork/elrond-go/storage/lrucache"
)

// FullHistoryPruningStorer represents a storer for full history nodes
// which creates a new persister for each epoch and removes older activePersisters
type FullHistoryPruningStorer struct {
	*PruningStorer
	args                      *StorerArgs
	shardId                   string
	pathManager               storage.PathManagerHandler
	oldEpochsActivePersisters storage.Cacher
}

// NewFullHistoryPruningStorer will return a new instance of PruningStorer without sharded directories' naming scheme
func NewFullHistoryPruningStorer(args *FullHistoryStorerArgs) (*FullHistoryPruningStorer, error) {
	return initFullHistoryPruningStorer(args, "")
}

// NewShardedFullHistoryPruningStorer will return a new instance of PruningStorer with sharded directories' naming scheme
func NewShardedFullHistoryPruningStorer(
	args *FullHistoryStorerArgs,
	shardID uint32,
) (*FullHistoryPruningStorer, error) {
	shardStr := fmt.Sprintf("%d", shardID)
	return initFullHistoryPruningStorer(args, shardStr)
}

func initFullHistoryPruningStorer(args *FullHistoryStorerArgs, shardId string) (*FullHistoryPruningStorer, error) {
	ps, err := initPruningStorer(args.StorerArgs, shardId)
	if err != nil {
		return nil, err
	}

	if args.NumOfOldActivePersisters < 1 || args.NumOfOldActivePersisters > math.MaxInt32 {
		return nil, err
	}

	oldEpochsActivePersisters, err := lrucache.NewCacheWithEviction(int(args.NumOfOldActivePersisters),
		func(key interface{}, value interface{}) {
			pd, ok := value.(persisterData)
			if ok {
				err := pd.Close()
				if err != nil {
					log.Warn("initFullHistoryPruningStorer - eviction", "err", err.Error())
				}
			}
		})

	if err != nil {
		return nil, err
	}

	return &FullHistoryPruningStorer{
		PruningStorer:             ps,
		args:                      args.StorerArgs,
		shardId:                   shardId,
		pathManager:               args.PathManager,
		oldEpochsActivePersisters: oldEpochsActivePersisters,
	}, nil
}

// GetFromEpoch will search a key only in the persister for the given epoch
func (fhps *FullHistoryPruningStorer) GetFromEpoch(key []byte, epoch uint32) ([]byte, error) {
	// TODO: this will be used when requesting from resolvers
	v, ok := fhps.cacher.Get(key)
	if ok {
		return v.([]byte), nil
	}

	fhps.lock.RLock()
	pd, exists := fhps.persistersMapByEpoch[epoch]
	fhps.lock.RUnlock()
	if !exists {
		p, err := createPersisterDataForEpoch(fhps.args, epoch, fhps.shardId)
		if err != nil {
			return nil, err
		}
		epochString := fmt.Sprintf("%d", epoch)
		fhps.oldEpochsActivePersisters.Put([]byte(epochString), p, 0)
	}

	persister, closePersister, err := fhps.createAndInitPersisterIfClosed(pd)
	if err != nil {
		return nil, err
	}
	defer closePersister()

	res, err := persister.Get(key)
	if err == nil {
		return res, nil
	}

	log.Warn("get from closed persister",
		"id", fhps.identifier,
		"epoch", epoch,
		"key", key,
		"error", err.Error())

	return nil, fmt.Errorf("key %s not found in %s",
		hex.EncodeToString(key), fhps.identifier)

}
