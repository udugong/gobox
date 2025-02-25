package gormx

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/udugong/limiter"
	"github.com/udugong/limiter/activelimit"
)

func TestRateLimitCallbacks_key(t *testing.T) {
	tests := []struct {
		name   string
		suffix string
		want   string
	}{
		{
			name:   "normal",
			suffix: "",
			want:   "gorm_db_limiter",
		},
		{
			name:   "use_suffix",
			suffix: string(OperationCreate),
			want:   "gorm_db_limiter_create",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &RateLimitCallbacks{}
			got := c.key(tt.suffix)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWithOperationLimiter(t *testing.T) {
	l := activelimit.NewLocalActiveLimiter(1)
	tests := []struct {
		name string
		op   OperationType
		l    limiter.Limiter
		want limiter.Limiter
	}{
		{
			name: "normal",
			op:   OperationCreate,
			l:    l,
			want: l,
		},
		{
			name: "nil",
			op:   "",
			l:    nil,
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewRateLimitCallbacks(nil, nil, WithOperationLimiter(tt.op, tt.l))
			assert.Equal(t, tt.want, c.limiters[tt.op])
		})
	}
}
