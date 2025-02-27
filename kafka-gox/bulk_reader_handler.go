package kafkax

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

type BulkHandler[T any] struct {
	batchSize  int
	interval   time.Duration
	decodeFn   func([]byte) (T, error)
	fn         func(ctx context.Context, msg []kafka.Message, t []T) error
	needClosed atomic.Bool
	logger     *slog.Logger
}

func NewBulkHandler[T any](logger *slog.Logger,
	fn func(ctx context.Context, msg []kafka.Message, t []T) error) *BulkHandler[T] {
	return &BulkHandler[T]{
		batchSize: 10,
		interval:  time.Second,
		decodeFn: func(data []byte) (T, error) {
			var t T
			return t, json.Unmarshal(data, &t)
		},
		fn:         fn,
		needClosed: atomic.Bool{},
		logger:     logger,
	}
}

func (h *BulkHandler[T]) SetDecodeFn(fn func([]byte) (T, error)) *BulkHandler[T] {
	h.decodeFn = fn
	return h
}

func (h *BulkHandler[T]) SetBatchSize(batchSize int) *BulkHandler[T] {
	h.batchSize = batchSize
	return h
}

func (h *BulkHandler[T]) SetInterval(interval time.Duration) *BulkHandler[T] {
	h.interval = interval
	return h
}

func (h *BulkHandler[T]) ReadMsg(ctx context.Context, r *kafka.Reader) error {
	for !h.needClosed.Load() {
		batch, ts, err := h.mergeMessage(ctx, r)
		if err != nil {
			return err
		}
		if len(ts) == 0 {
			continue
		}
		if err := h.fn(ctx, batch, ts); err != nil {
			h.logError(ctx, "批量处理消息失败", err, batch)
			continue
		}
		if err := r.CommitMessages(ctx, batch...); err != nil {
			h.logError(ctx, "批量处理消息成功但 kafka 提交消息失败", err, batch)
		}
	}
	return nil
}

func (h *BulkHandler[T]) mergeMessage(ctx context.Context, r *kafka.Reader) ([]kafka.Message, []T, error) {
	batch := make([]kafka.Message, 0, h.batchSize)
	ts := make([]T, 0, h.batchSize)
	ctx1, cancel1 := context.WithTimeout(ctx, h.interval)
	defer cancel1()
	done := false
	for i := 0; i < h.batchSize && !done; {
		msg, err := r.FetchMessage(ctx1)
		switch {
		case errors.Is(err, io.EOF):
			return batch, ts, ErrReaderClosed
		case errors.Is(err, context.Canceled):
			return batch, ts, ctx1.Err()
		case errors.Is(err, context.DeadlineExceeded):
			done = true
			continue
		case err != nil:
			h.logger.LogAttrs(ctx1, slog.LevelError, "kafka 获取消息失败",
				slog.Any("err", err))
			continue
		}
		t, err := h.decodeFn(msg.Value)
		if err != nil {
			h.logger.LogAttrs(ctx1, slog.LevelError, "反序列化消息失败",
				slog.String("topic", msg.Topic),
				slog.Int("partition", msg.Partition),
				slog.Int64("offset", msg.Offset),
				slog.Any("err", err),
			)
			continue
		}
		batch = append(batch, msg)
		ts = append(ts, t)
		i++
	}
	return batch, ts, nil
}

func (h *BulkHandler[T]) logError(ctx context.Context, msg string, err error, batch []kafka.Message) {
	h.logger.LogAttrs(ctx, slog.LevelError, msg,
		slog.String("topic", batch[0].Topic),
		slog.Int("partition", batch[0].Partition),
		slog.Int("msg_len", len(batch)),
		slog.Int64("start_offset", batch[0].Offset),
		slog.Int64("end_offset", batch[len(batch)-1].Offset),
		slog.Any("err", err),
	)
}

func (h *BulkHandler[T]) Close() {
	h.needClosed.Store(true)
}
