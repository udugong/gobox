package kafkax

import "errors"

var (
	ErrReaderClosed = errors.New("kafka reader closed")
)
