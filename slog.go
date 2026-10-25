package heterolog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sort"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
)

// SlogHandler adapts the standard library's structured logger to Logger.
// Derived handlers share the destination; closing its runtime remains the
// caller's responsibility. The destination must support concurrent calls.
type SlogHandler struct {
	logger Logger
	opts   slog.HandlerOptions
	groups []string
	bound  []slogAttributeBatch
	err    error
}

type slogAttributeBatch struct {
	groups []string
	attrs  map[string]any
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
	if logger == nil || isNilSlogLogger(logger) {
		panic("heterolog: NewSlogHandler requires a non-nil logger")
	}
	h := &SlogHandler{logger: logger}
	if opts != nil {
		h.opts = *opts
	}
	return h
}

func isNilSlogLogger(logger Logger) bool {
	value := reflect.ValueOf(logger)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Enabled reports whether level meets the configured minimum. A LevelVar in the
// options is consulted on every call, allowing changes without rebuilding the
// handler. Each backend may impose its own additional level filter.
func (h *SlogHandler) Enabled(_ context.Context, level slog.Level) bool {
	minimum := slog.LevelInfo
	if h.opts.Level != nil {
		minimum = h.opts.Level.Level()
	}
	return level >= minimum
}

// Handle sends a record to the destination and returns any delivery error. Like
// the standard handlers, Handle does not apply Enabled itself. Context values
// (including trace and request IDs) are retained; cancellation does not suppress
// the log record.
func (h *SlogHandler) Handle(ctx context.Context, record slog.Record) error {
	if h.err != nil {
		return h.err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = applogging.WithSlogRecordTime(context.WithoutCancel(ctx), record.Time)

	builtins := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	fields, message, err := h.encodeBuiltins(builtins)
	if err != nil {
		return err
	}
	groupTargets := []map[string]any{fields}
	for _, batch := range h.bound {
		addSlogAttributes(&groupTargets, batch.groups, batch.attrs)
	}
	attrs := make([]slog.Attr, 0, record.NumAttrs())
	record.Attrs(func(attr slog.Attr) bool {
		attrs = append(attrs, attr)
		return true
	})
	values, err := h.encodeAttributes(attrs)
	if err != nil {
		return err
	}
	addSlogAttributes(&groupTargets, h.groups, values)

	// Stable order keeps field snapshots and size validation deterministic.
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	output := make([]Field, 0, len(keys))
	for _, key := range keys {
		field, err := applogging.NewSlogField(key, fields[key])
		if err != nil {
			return err
		}
		output = append(output, field)
	}

	switch {
	case record.Level < slog.LevelInfo:
		return h.logger.Debug(ctx, message, output...)
	case record.Level < slog.LevelWarn:
		return h.logger.Info(ctx, message, output...)
	case record.Level < slog.LevelError:
		return h.logger.Warn(ctx, message, output...)
	default:
		return h.logger.Error(ctx, message, output...)
	}
}

// WithAttrs returns an independent handler with a snapshot of attrs in the
// current group. LogValuer and ReplaceAttr are evaluated here, once.
func (h *SlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	clone := *h
	values, err := h.encodeAttributes(attrs)
	if err != nil {
		clone.err = err
		return &clone
	}
	if len(values) == 0 {
		return h
	}
	clone.bound = append(slices.Clone(h.bound), slogAttributeBatch{
		groups: slices.Clone(h.groups),
		attrs:  values,
	})
	return &clone
}

// WithGroup returns an independent handler whose subsequent attributes belong
// to name. Empty names have no effect.
func (h *SlogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	clone := *h
	clone.groups = append(slices.Clone(h.groups), name)
	return &clone
}

// encodeBuiltins also obtains the backend message from its encoded replacement.
// Reading normalized JSON avoids falling back to the original message or
// invoking String methods that could disclose fields omitted by JSON encoding.
func (h *SlogHandler) encodeBuiltins(record slog.Record) (map[string]any, string, error) {
	opts := h.opts
	messageKey := slog.MessageKey
	keepMessage := true
	if replace := opts.ReplaceAttr; replace != nil {
		opts.ReplaceAttr = func(groups []string, attr slog.Attr) slog.Attr {
			replacement := replace(groups, attr)
			if len(groups) == 0 && attr.Key == slog.MessageKey {
				messageKey = replacement.Key
				keepMessage = !replacement.Equal(slog.Attr{})
			}
			return replacement
		}
	}
	fields, err := encodeSlogRecord(record, &opts)
	if err != nil {
		return nil, "", err
	}
	if !keepMessage {
		return fields, "", nil
	}
	value, exists := fields[messageKey]
	if !exists {
		return fields, "", nil
	}
	if message, ok := value.(string); ok {
		return fields, message, nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, "", fmt.Errorf("heterolog: encode replaced slog message: %w", err)
	}
	return fields, string(encoded), nil
}

// encodeAttributes delegates value resolution and JSON semantics to the
// standard handler. The private enclosing group separates attributes from its
// builtins, and is removed before exposing group paths to ReplaceAttr.
func (h *SlogHandler) encodeAttributes(attrs []slog.Attr) (map[string]any, error) {
	if len(attrs) == 0 {
		return nil, nil
	}
	const envelope = "heterolog_attributes"
	opts := h.opts
	opts.ReplaceAttr = func(groups []string, attr slog.Attr) slog.Attr {
		if len(groups) == 0 {
			return slog.Attr{}
		}
		if h.opts.ReplaceAttr == nil {
			return attr
		}
		path := make([]string, 0, len(h.groups)+len(groups)-1)
		path = append(path, h.groups...)
		path = append(path, groups[1:]...)
		return h.opts.ReplaceAttr(path, attr)
	}
	var record slog.Record
	record.AddAttrs(slog.Attr{Key: envelope, Value: slog.GroupValue(attrs...)})
	values, err := encodeSlogRecord(record, &opts)
	if err != nil {
		return nil, err
	}
	result, _ := values[envelope].(map[string]any)
	return result, nil
}

func encodeSlogRecord(record slog.Record, opts *slog.HandlerOptions) (map[string]any, error) {
	var buffer bytes.Buffer
	if err := slog.NewJSONHandler(&buffer, opts).Handle(context.Background(), record); err != nil {
		return nil, fmt.Errorf("heterolog: encode slog record: %w", err)
	}
	decoder := json.NewDecoder(&buffer)
	decoder.UseNumber()
	var result map[string]any
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("heterolog: decode slog record: %w", err)
	}
	return result, nil
}

func addSlogAttributes(targets *[]map[string]any, groups []string, attrs map[string]any) {
	if len(attrs) == 0 {
		return
	}
	// WithGroup paths only grow along a derived handler's history. Opening a
	// new group replaces a previous attribute with that name; continuing an
	// already-open group retains its bound attributes.
	for len(*targets) <= len(groups) {
		parent := (*targets)[len(*targets)-1]
		next := make(map[string]any)
		parent[groups[len(*targets)-1]] = next
		*targets = append(*targets, next)
	}
	target := (*targets)[len(groups)]
	for key, value := range attrs {
		target[key] = cloneSlogValue(value)
	}
}

func cloneSlogValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		clone := make(map[string]any, len(value))
		for key, item := range value {
			clone[key] = cloneSlogValue(item)
		}
		return clone
	case []any:
		clone := make([]any, len(value))
		for index, item := range value {
			clone[index] = cloneSlogValue(item)
		}
		return clone
	default:
		return value
	}
}
