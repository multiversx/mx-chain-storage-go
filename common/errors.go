package common

import "errors"

// ErrDBIsClosed is raised when the DB is closed
var ErrDBIsClosed = errors.New("DB is closed")
