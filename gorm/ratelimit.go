package gormx

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/udugong/limiter"
	"gorm.io/gorm"
)

var ErrGormTooManyRequests = errors.New("gorm 请求过多")

// OperationType 定义操作类型
type OperationType string

const (
	OperationCreate OperationType = "create"
	OperationUpdate OperationType = "update"
	OperationDelete OperationType = "delete"
	OperationQuery  OperationType = "query"
	OperationRaw    OperationType = "raw"
	OperationRow    OperationType = "row"
)

type RateLimitCallbacks struct {
	globalLimiter limiter.Limiter
	limiters      map[OperationType]limiter.Limiter
	logger        *slog.Logger
}

func NewRateLimitCallbacks(l limiter.Limiter, logger *slog.Logger,
	opts ...RateLimitCallbacksOption) *RateLimitCallbacks {
	c := &RateLimitCallbacks{
		globalLimiter: l,
		limiters:      make(map[OperationType]limiter.Limiter, 6),
		logger:        logger,
	}
	for _, opt := range opts {
		opt.apply(c)
	}
	return c
}

type RateLimitCallbacksOption interface {
	apply(*RateLimitCallbacks)
}

type rateLimitCallbacksOptionFunc func(*RateLimitCallbacks)

func (f rateLimitCallbacksOptionFunc) apply(r *RateLimitCallbacks) {
	f(r)
}

// WithOperationLimiter 按操作类型设置限流器
func WithOperationLimiter(op OperationType, l limiter.Limiter) RateLimitCallbacksOption {
	return rateLimitCallbacksOptionFunc(func(c *RateLimitCallbacks) {
		c.limiters[op] = l
	})
}

func (c *RateLimitCallbacks) Name() string {
	return "rate_limiter"
}

func (c *RateLimitCallbacks) Initialize(db *gorm.DB) error {
	err := db.Callback().Create().Before("*").
		Register("rate_limiter_before_create", c.beforeCreate)
	if err != nil {
		return err
	}

	err = db.Callback().Update().Before("*").
		Register("rate_limiter_before_update", c.beforeUpdate)
	if err != nil {
		return err
	}

	err = db.Callback().Delete().Before("*").
		Register("rate_limiter_before_delete", c.beforeDelete)
	if err != nil {
		return err
	}

	err = db.Callback().Query().Before("*").
		Register("rate_limiter_before_query", c.beforeQuery)
	if err != nil {
		return err
	}

	err = db.Callback().Raw().Before("*").
		Register("rate_limiter_before_raw", c.beforeRaw)
	if err != nil {
		return err
	}

	err = db.Callback().Row().Before("*").
		Register("rate_limiter_before_row", c.beforeRow)
	if err != nil {
		return err
	}
	return nil
}

func (c *RateLimitCallbacks) limit(db *gorm.DB, op OperationType) {
	ctx := db.Statement.Context
	l := c.globalLimiter
	suffix := ""
	if limit, ok := c.limiters[op]; ok {
		l = limit
		suffix = string(op)
	}
	limited, err := l.Limit(ctx, c.key(suffix))
	if err != nil {
		c.logger.LogAttrs(ctx, slog.LevelError, "限流器异常", slog.Any("err", err))
		limited = true
	}
	if limited {
		_ = db.AddError(fmt.Errorf("%s 操作触发限流; err: %v", op, ErrGormTooManyRequests))
	}
}

func (c *RateLimitCallbacks) key(suffix string) string {
	const baseKey = "gorm_db_limiter"
	var b strings.Builder
	b.Grow(22)
	b.WriteString(baseKey)
	if suffix != "" {
		b.WriteString("_")
		b.WriteString(suffix)
	}
	return b.String()
}

func (c *RateLimitCallbacks) beforeCreate(db *gorm.DB) {
	c.limit(db, OperationCreate)
}

func (c *RateLimitCallbacks) beforeUpdate(db *gorm.DB) {
	c.limit(db, OperationUpdate)
}

func (c *RateLimitCallbacks) beforeDelete(db *gorm.DB) {
	c.limit(db, OperationDelete)
}

func (c *RateLimitCallbacks) beforeQuery(db *gorm.DB) {
	c.limit(db, OperationQuery)
}

func (c *RateLimitCallbacks) beforeRaw(db *gorm.DB) {
	c.limit(db, OperationRaw)
}

func (c *RateLimitCallbacks) beforeRow(db *gorm.DB) {
	c.limit(db, OperationRow)
}
