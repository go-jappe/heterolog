package tracer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/application/trace/span"
)

// tracerImpl はTracerインターフェースの実装
type tracerImpl struct {
	mu       sync.RWMutex
	spans    []span.SpanInput
	sampler  Sampler
	exporter SpanExporter
}

// NewTracer は新しいTracerを作成する
func NewTracer(opts ...TracerOption) Tracer {
	t := &tracerImpl{
		spans:   make([]span.SpanInput, 0),
		sampler: AlwaysSample(), // デフォルトは常にサンプリング
	}

	for _, opt := range opts {
		opt(t)
	}

	return t
}

// TracerOption はTracer作成時のオプション
type TracerOption func(*tracerImpl)

// WithSampler はサンプラーを設定する
func WithSampler(sampler Sampler) TracerOption {
	return func(t *tracerImpl) {
		t.sampler = sampler
	}
}

// WithExporter はエクスポーターを設定する
func WithExporter(exporter SpanExporter) TracerOption {
	return func(t *tracerImpl) {
		t.exporter = exporter
	}
}

// StartSpan は新しいSpanを開始する
func (t *tracerImpl) StartSpan(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span) {
	options := &spanOptions{
		kind:      span.KindInternal,
		startTime: time.Now(),
	}

	for _, opt := range opts {
		opt(options)
	}

	// 親Spanの取得
	var parentSpanContext *SpanContext
	if parentSpan := SpanFromContext(ctx); parentSpan != nil {
		psc := parentSpan.SpanContext()
		parentSpanContext = &psc
	}

	// サンプリング判定
	sampled := t.sampler.ShouldSample(SamplingParameters{
		ParentContext: parentSpanContext,
		Name:          name,
		Kind:          options.kind,
		Attributes:    options.attributes,
	})

	spanCtx := SpanContext{
		TraceID:    GenerateTraceID(),
		SpanID:     GenerateSpanID(),
		TraceFlags: 0,
	}

	if sampled {
		spanCtx.TraceFlags = 1 // サンプリングフラグ
	}

	// 親Spanがある場合はTraceIDを引き継ぐ
	if parentSpanContext != nil {
		spanCtx.TraceID = parentSpanContext.TraceID
	}

	s := &spanImpl{
		tracer:     t,
		name:       name,
		kind:       options.kind,
		startTime:  options.startTime,
		attributes: options.attributes,
		events:     make([]span.SpanEvent, 0),
		links:      options.links,
		status: span.SpanStatus{
			Code: span.StatusUnset,
		},
		spanContext: spanCtx,
		sampled:     sampled,
		recording:   true,
	}

	// コンテキストに新しいSpanを設定
	ctx = ContextWithSpan(ctx, s)

	return ctx, s
}

// Extract はコンテキストから現在のSpanを取得する
func (t *tracerImpl) Extract(ctx context.Context) Span {
	return SpanFromContext(ctx)
}

// export はSpanをエクスポートする（内部メソッド）
func (t *tracerImpl) export(s *spanImpl) {
	if !s.sampled || t.exporter == nil {
		return
	}

	spanInput := span.SpanInput{
		Name:                   s.name,
		Kind:                   s.kind,
		StartTime:              s.startTime,
		EndTime:                s.endTime,
		Attributes:             s.attributes,
		Events:                 s.events,
		Status:                 s.status,
		Links:                  s.links,
		Sampled:                s.sampled,
		DroppedAttributesCount: s.droppedAttributesCount,
		DroppedEventsCount:     s.droppedEventsCount,
		DroppedLinksCount:      s.droppedLinksCount,
	}

	t.mu.Lock()
	t.spans = append(t.spans, spanInput)
	t.mu.Unlock()

	// 非同期でエクスポート
	go func() {
		if err := t.exporter.Export(context.Background(), []span.SpanInput{spanInput}); err != nil {
			// エラーログを出力（実際の実装ではロガーを使用）
			fmt.Printf("Failed to export span: %v\n", err)
		}
	}()
}

// GetSpans は記録されたSpanを取得する（テスト用）
func (t *tracerImpl) GetSpans() []span.SpanInput {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]span.SpanInput, len(t.spans))
	copy(result, t.spans)
	return result
}
