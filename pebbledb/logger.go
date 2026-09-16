package pebbledb

import (
	"fmt"

	logger "github.com/multiversx/mx-chain-logger-go"
)

var log = logger.GetOrCreate("storage/pebbledb")

// pebbleLogger routes pebble's internal logging into the node logger instead of stderr
type pebbleLogger struct{}

// Infof -
func (pebbleLogger) Infof(format string, args ...interface{}) {
	log.Trace(fmt.Sprintf(format, args...))
}

// Errorf -
func (pebbleLogger) Errorf(format string, args ...interface{}) {
	log.Error(fmt.Sprintf(format, args...))
}

// Fatalf -
func (pebbleLogger) Fatalf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	log.Error(message)
	panic(message)
}
