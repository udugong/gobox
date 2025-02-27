package slidewindow

import (
	"context"
	"log/slog"

	"github.com/udugong/limiter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Limiter struct {
	limiter limiter.Limiter
	key     string
	logger  *slog.Logger
}

func NewLimiter(limiter limiter.Limiter, key string, logger *slog.Logger) *Limiter {
	return &Limiter{limiter: limiter, key: key, logger: logger}
}

func (l *Limiter) BuildUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (resp any, err error) {
		isLimited, err := l.limiter.Limit(ctx, l.key)
		if err != nil {
			l.logger.LogAttrs(ctx, slog.LevelError, "限流器异常", slog.Any("err", err))
			return nil, status.Error(codes.ResourceExhausted, "触发限流")
		}
		if isLimited {
			return nil, status.Error(codes.ResourceExhausted, "触发限流")
		}
		return handler(ctx, req)
	}
}
