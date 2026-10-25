// otel sdk使う予定のためこの自作は廃止予定
package tracer

import (
	"context"
	"github.com/go-jappe/heterolog/internal/application/trace/span"
	"time"

	"github.com/google/uuid"
)

// Tracer はトレース機能を提供するインターフェース
type Tracer interface {
	// StartSpan は新しいSpanを開始する
	StartSpan(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span)

	// Extract はコンテキストから現在のSpanを取得する
	Extract(ctx context.Context) Span
}

// Span は個別のトレース区間を表すインターフェース
type Span interface {
	// End はSpanを終了する
	End()

	// SetAttributes は属性を設定する
	SetAttributes(attrs ...span.Attribute)

	// AddEvent はイベントを追加する
	AddEvent(name string, attrs ...span.Attribute)

	// SetStatus はステータスを設定する
	SetStatus(code span.StatusCode, message string)

	// RecordError はエラーを記録する
	RecordError(err error)

	// IsRecording はSpanが記録中かどうかを返す
	IsRecording() bool

	// SpanContext はSpanのコンテキスト情報を返す
	SpanContext() SpanContext
}

// SpanContext はSpanのコンテキスト情報
type SpanContext struct {
	TraceID    string
	SpanID     string
	TraceFlags uint8
	TraceState string
}

// SpanOption はSpan作成時のオプション
type SpanOption func(*spanOptions)

type spanOptions struct {
	kind       span.SpanKind
	attributes []span.Attribute
	links      []span.SpanLink
	startTime  time.Time
}

// WithSpanKind はSpanの種類を設定する
func WithSpanKind(kind span.SpanKind) SpanOption {
	return func(opts *spanOptions) {
		opts.kind = kind
	}
}

// WithAttributes は初期属性を設定する
func WithAttributes(attrs ...span.Attribute) SpanOption {
	return func(opts *spanOptions) {
		opts.attributes = append(opts.attributes, attrs...)
	}
}

// WithLinks は関連Spanを設定する
func WithLinks(links ...span.SpanLink) SpanOption {
	return func(opts *spanOptions) {
		opts.links = append(opts.links, links...)
	}
}

// WithStartTime は開始時刻を設定する
func WithStartTime(t time.Time) SpanOption {
	return func(opts *spanOptions) {
		opts.startTime = t
	}
}

// contextKey はコンテキストに格納するためのキー
type contextKey struct{}

var activeSpanKey = contextKey{}

// ContextWithSpan はコンテキストにSpanを格納する
func ContextWithSpan(ctx context.Context, span Span) context.Context {
	return context.WithValue(ctx, activeSpanKey, span)
}

// SpanFromContext はコンテキストからSpanを取得する
func SpanFromContext(ctx context.Context) Span {
	if span, ok := ctx.Value(activeSpanKey).(Span); ok {
		return span
	}
	return nil
}

// GenerateTraceID は新しいトレースIDを生成する
func GenerateTraceID() string {
	return uuid.New().String()
}

// GenerateSpanID は新しいSpanIDを生成する
func GenerateSpanID() string {
	return uuid.New().String()[:16] // 16文字に短縮
}
