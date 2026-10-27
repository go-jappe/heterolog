package logger

import "context"

// Logger synchronously records structured entries and reports failures. The
// trace and span IDs come from ctx unless fields named trace_id or span_id set
// them, and other details, such as the request ID, come from fields.
type Logger interface {
	Debug(ctx context.Context, message string, fields ...Field) error
	Info(ctx context.Context, message string, fields ...Field) error
	Warn(ctx context.Context, message string, fields ...Field) error
	Error(ctx context.Context, message string, fields ...Field) error
	ErrorWithStack(ctx context.Context, message string, errorMessage error, fields ...Field) error
}

// AsyncLogger accepts entries without waiting for them to be processed. It
// does not return delivery errors.
type AsyncLogger interface {
	Debug(ctx context.Context, message string, fields ...Field)
	Info(ctx context.Context, message string, fields ...Field)
	Warn(ctx context.Context, message string, fields ...Field)
	Error(ctx context.Context, message string, fields ...Field)
	ErrorWithStack(ctx context.Context, message string, errorMessage error, fields ...Field)
}

// DeliveryLogger returns a DeliveryReport with the result for each configured
// destination. Saving an entry after a failed delivery, for example to an
// archive, does not count as delivering it.
type DeliveryLogger interface {
	Debug(ctx context.Context, message string, fields ...Field) (DeliveryReport, error)
	Info(ctx context.Context, message string, fields ...Field) (DeliveryReport, error)
	Warn(ctx context.Context, message string, fields ...Field) (DeliveryReport, error)
	Error(ctx context.Context, message string, fields ...Field) (DeliveryReport, error)
	ErrorWithStack(ctx context.Context, message string, errorMessage error, fields ...Field) (DeliveryReport, error)
}
