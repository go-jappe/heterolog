package logging

import (
	"context"
	"time"
)

type slogRecordTimeKey struct{}

// WithSlogRecordTimeは、呼び出し元のcontextの値を変えずに、slogのrecordの元の時刻を
// 既存のlogging APIへ引き継ぎます。
func WithSlogRecordTime(ctx context.Context, occurredAt time.Time) context.Context {
	return context.WithValue(ctx, slogRecordTimeKey{}, occurredAt)
}

// SlogRecordTimeは、slogの受け口が渡した事象の時刻を返します。
func SlogRecordTime(ctx context.Context) (time.Time, bool) {
	if ctx == nil {
		return time.Time{}, false
	}
	occurredAt, ok := ctx.Value(slogRecordTimeKey{}).(time.Time)
	return occurredAt, ok
}
