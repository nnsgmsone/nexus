package defines

import "errors"

const (
	DefaultRows = 8192
)

var (
	ErrInvalidLog = errors.New("invalid log")
)
