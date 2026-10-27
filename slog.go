package heterolog

import (
	"context"
	"log/slog"

	"github.com/go-jappe/heterolog/internal/presentation/sloghandler"
)

// SlogHandler adapts the standard library's structured logger to Logger.
// Derived handlers share the destination; closing its runtime remains the
// caller's responsibility. The destination must support concurrent calls.
type SlogHandler struct {
	// handlerは処理を受け持つ内部のhandlerです。zero値のSlogHandlerではnilです。
	handler *sloghandler.Handler
}

var _ slog.Handler = (*SlogHandler)(nil)

// NewSlogHandler returns a handler suitable for slog.New. A nil options pointer
// uses slog's defaults (Info level, no source). A nil logger panics.
//
// Attributes follow slog.JSONHandler's encoding and ReplaceAttr behavior. The
// complete slog record, including time, level, msg and optional source, is
// available in the destination's structured fields. Duplicate keys retain their
// last value. The original record level determines backend routing, and its
// original time is also used as the backend event time. ReplaceAttr changes the
// structured fields and the backend's message, but not routing or event time.
// A removed message becomes empty; a non-string message uses its JSON encoding.
// String-valued top-level request_id, trace_id and span_id become event metadata.
// Non-string values remain ordinary structured fields alongside any context IDs.
//
// slog.Logger discards errors returned by Handler.Handle. Use the native logging
// API for returned errors, or NewDelivery for per-destination delivery reports.
func NewSlogHandler(logger Logger, opts *slog.HandlerOptions) *SlogHandler {
	return &SlogHandler{handler: sloghandler.New(logger, opts)}
}

// Enabled reports whether level meets the configured minimum. A LevelVar in the
// options is consulted on every call, allowing changes without rebuilding the
// handler. Each backend may impose its own additional level filter.
func (h *SlogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.impl().Enabled(ctx, level)
}

// Handle sends a record to the destination and returns any delivery error. Like
// the standard handlers, Handle does not apply Enabled itself. Context values
// (including trace and request IDs) are retained; cancellation does not suppress
// the log record.
func (h *SlogHandler) Handle(ctx context.Context, record slog.Record) error {
	return h.impl().Handle(ctx, record)
}

// WithAttrs returns an independent handler with a snapshot of attrs in the
// current group. LogValuer and ReplaceAttr are evaluated here, once.
func (h *SlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return newSlogHandler(h.impl().WithAttrs(attrs))
}

// WithGroup returns an independent handler whose subsequent attributes belong
// to name. Empty names have no effect.
func (h *SlogHandler) WithGroup(name string) slog.Handler {
	return newSlogHandler(h.impl().WithGroup(name))
}

// newSlogHandlerは、内部のhandlerから派生したhandlerを公開の型で包みます。内部の
// WithAttrsとWithGroupは、常に*sloghandler.Handlerを返します。
func newSlogHandler(handler slog.Handler) *SlogHandler {
	return &SlogHandler{handler: handler.(*sloghandler.Handler)}
}

// implは、処理を渡す内部のhandlerを返します。zero値のSlogHandlerには内部のhandlerの
// zero値を返し、内部の型のzero値と同じく振る舞うようにします。
func (h *SlogHandler) impl() *sloghandler.Handler {
	if h.handler == nil {
		return &sloghandler.Handler{}
	}
	return h.handler
}
