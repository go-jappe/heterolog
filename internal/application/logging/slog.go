package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// NewSlogField snapshots a JSON-compatible slog attribute. Unlike the public
// field constructors, this internal adapter accepts arbitrary attribute keys,
// including the empty key, as required by log/slog. Like Any, it rejects values
// nested deeper than MaxFieldNestingDepth with ErrFieldValueNotJSON.
func NewSlogField(key string, value any) (Field, error) {
	encoded, err := json.Marshal(value)
	if err == nil {
		err = validateNestingDepth(encoded)
	}
	if err != nil {
		// JSON化のerrorは記録しようとした値やMarshalJSONが返したerrorを保持し得るため、
		// errors.Asで取り出せないよう原因としては包みません。種類はErrFieldValueNotJSONで判定します。
		return Field{}, fmt.Errorf("%w: slog attribute %q: %v", ErrFieldValueNotJSON, key, err)
	}
	return Field{
		key:             key,
		value:           jsonSnapshot(encoded),
		emptyKeyDefined: key == "",
	}, nil
}

type slogRecordTimeKey struct{}

// WithSlogRecordTime preserves the original event time across the existing
// application logging API without changing the caller's context values.
func WithSlogRecordTime(ctx context.Context, occurredAt time.Time) context.Context {
	return context.WithValue(ctx, slogRecordTimeKey{}, occurredAt)
}

// SlogRecordTime reports an occurrence time supplied by the slog adapter.
func SlogRecordTime(ctx context.Context) (time.Time, bool) {
	if ctx == nil {
		return time.Time{}, false
	}
	occurredAt, ok := ctx.Value(slogRecordTimeKey{}).(time.Time)
	return occurredAt, ok
}
