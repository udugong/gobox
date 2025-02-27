package kafkax

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Reader interface {
	ReadMsg(ctx context.Context, r *kafka.Reader) error
	Close()
}
