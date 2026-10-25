package tracer

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/trace/span"
)

// SpanExporter はSpanをエクスポートするインターフェース
type SpanExporter interface {
	// Export はSpanをエクスポートする
	Export(ctx context.Context, spans []span.SpanInput) error

	// Shutdown はエクスポーターをシャットダウンする
	Shutdown(ctx context.Context) error
}

// ConsoleExporter はコンソールにSpanを出力するエクスポーター
type ConsoleExporter struct{}

// Export はSpanをコンソールに出力する
func (e *ConsoleExporter) Export(ctx context.Context, spans []span.SpanInput) error {
	// 実際の実装では、ここでコンソールに出力する
	// 今回は簡略化のため、何もしない
	return nil
}

// Shutdown はエクスポーターをシャットダウンする
func (e *ConsoleExporter) Shutdown(ctx context.Context) error {
	return nil
}

// NewConsoleExporter は新しいConsoleExporterを作成する
func NewConsoleExporter() SpanExporter {
	return &ConsoleExporter{}
}

// NoopExporter は何もしないエクスポーター
type NoopExporter struct{}

// Export は何もしない
func (e *NoopExporter) Export(ctx context.Context, spans []span.SpanInput) error {
	return nil
}

// Shutdown は何もしない
func (e *NoopExporter) Shutdown(ctx context.Context) error {
	return nil
}

// NewNoopExporter は新しいNoopExporterを作成する
func NewNoopExporter() SpanExporter {
	return &NoopExporter{}
}

// BatchExporter はバッチでSpanをエクスポートするエクスポーター
type BatchExporter struct {
	exporter SpanExporter
	// 実際の実装では、バッチ処理のための設定を持つ
}

// Export はSpanをバッチでエクスポートする
func (e *BatchExporter) Export(ctx context.Context, spans []span.SpanInput) error {
	// 実際の実装では、ここでバッチ処理を行う
	return e.exporter.Export(ctx, spans)
}

// Shutdown はエクスポーターをシャットダウンする
func (e *BatchExporter) Shutdown(ctx context.Context) error {
	return e.exporter.Shutdown(ctx)
}

// NewBatchExporter は新しいBatchExporterを作成する
func NewBatchExporter(exporter SpanExporter) SpanExporter {
	return &BatchExporter{
		exporter: exporter,
	}
}
