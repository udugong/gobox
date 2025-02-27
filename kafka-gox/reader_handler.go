package kafkax

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"

	"github.com/segmentio/kafka-go"
)

type Handler[T any] struct {
	decodeFn   func([]byte) (T, error)
	fn         func(ctx context.Context, msg kafka.Message, t T) error
	needClosed atomic.Bool
	logger     *slog.Logger
}

func NewHandler[T any](logger *slog.Logger,
	fn func(ctx context.Context, msg kafka.Message, t T) error) *Handler[T] {
	return &Handler[T]{
		decodeFn: func(data []byte) (T, error) {
			var t T
			return t, json.Unmarshal(data, &t)
		},
		fn:         fn,
		needClosed: atomic.Bool{},
		logger:     logger,
	}
}

func (h *Handler[T]) SetDecodeFn(fn func([]byte) (T, error)) *Handler[T] {
	h.decodeFn = fn
	return h
}

func (h *Handler[T]) ReadMsg(ctx context.Context, r *kafka.Reader) error {
	for !h.needClosed.Load() {
		msg, err := r.FetchMessage(ctx)
		switch {
		case errors.Is(err, io.EOF):
			return ErrReaderClosed
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return ctx.Err()
		case err != nil:
			h.logError(ctx, "kafka 获取消息失败", err, msg)
			continue
		}

		t, err := h.decodeFn(msg.Value)
		if err != nil {
			h.logError(ctx, "反序列化消息失败", err, msg)
			continue
		}

		if err := h.fn(ctx, msg, t); err != nil {
			h.logError(ctx, "处理消息失败", err, msg)
			continue
		}

		if err := r.CommitMessages(ctx, msg); err != nil {
			h.logError(ctx, "处理消息成功但 kafka 提交消息失败", err, msg)
		}
	}
	return nil
}

func (h *Handler[T]) logError(ctx context.Context, msg string, err error, kafkaMsg kafka.Message) {
	h.logger.LogAttrs(ctx, slog.LevelError, msg,
		slog.String("topic", kafkaMsg.Topic),
		slog.Int("partition", kafkaMsg.Partition),
		slog.Int64("offset", kafkaMsg.Offset),
		slog.Any("err", err),
	)
}

func (h *Handler[T]) Close() {
	h.needClosed.Store(true)
}
