package pebbledb

import (
	"sync"
)

// accessGuard lets Close wait for the in-flight engine calls (pebble panics on use after Close) with
// no lock held across them: a call starting while Close is pending fails fast instead of deadlocking
type accessGuard struct {
	mut      sync.Mutex
	cond     *sync.Cond
	inFlight int
	closed   bool
}

func newAccessGuard() *accessGuard {
	guard := &accessGuard{}
	guard.cond = sync.NewCond(&guard.mut)

	return guard
}

// acquire registers an engine call; it returns false once closing started
func (g *accessGuard) acquire() bool {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.closed {
		return false
	}
	g.inFlight++

	return true
}

func (g *accessGuard) release() {
	g.mut.Lock()
	g.inFlight--
	if g.inFlight == 0 {
		g.cond.Broadcast()
	}
	g.mut.Unlock()
}

func (g *accessGuard) isClosed() bool {
	g.mut.Lock()
	defer g.mut.Unlock()

	return g.closed
}

// closeAndWait marks the guard closed and waits for the in-flight calls; it returns false when
// the guard was already closed, so only one caller performs the engine close
func (g *accessGuard) closeAndWait() bool {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.closed {
		return false
	}
	g.closed = true
	for g.inFlight > 0 {
		g.cond.Wait()
	}

	return true
}
