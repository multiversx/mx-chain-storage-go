package leveldb

import (
	"github.com/multiversx/mx-chain-go-storage/common"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type putBatchAct struct {
	batch   *batch
	resChan chan<- error
}

type pairResult struct {
	value []byte
	err   error
}

type serialQueryer interface {
	request(s *SerialDB)
}

type getAct struct {
	key     []byte
	resChan chan<- *pairResult
}

type hasAct struct {
	key     []byte
	resChan chan<- error
}

func (p *putBatchAct) request(s *SerialDB) {
	p.resChan <- p.doPutRequest(s)
}

func (p *putBatchAct) doPutRequest(s *SerialDB) error {
	db := s.getDbPointer()
	if db == nil {
		return common.ErrDBIsClosed
	}

	wopt := &opt.WriteOptions{
		Sync: true,
	}

	return db.Write(p.batch.batch, wopt)
}

func (g *getAct) request(s *SerialDB) {
	data, err := g.doGetRequest(s)

	res := &pairResult{
		value: data,
		err:   err,
	}
	g.resChan <- res
}

func (g *getAct) doGetRequest(s *SerialDB) ([]byte, error) {
	db := s.getDbPointer()
	if db == nil {
		return nil, common.ErrDBIsClosed
	}

	return db.Get(g.key, nil)
}

func (h *hasAct) request(s *SerialDB) {
	has, err := h.doHasRequest(s)

	if err != nil {
		h.resChan <- err
		return
	}

	if has {
		h.resChan <- nil
		return
	}

	h.resChan <- common.ErrKeyNotFound
}

func (h *hasAct) doHasRequest(s *SerialDB) (bool, error) {
	db := s.getDbPointer()
	if db == nil {
		return false, common.ErrDBIsClosed
	}

	return db.Has(h.key, nil)
}
