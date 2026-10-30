// Package jsonlinesは、spanのbatchをOTLP/JSONの1行にして書く送り先（fileとstdout）です。
package jsonlines

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces/otlp"
)

// metricsのexporter属性の値です。
const (
	FileExporterName   = "file"
	StdoutExporterName = "stdout"
)

// Exporterは、traces.Exporterの実装です。
type Exporter struct {
	name     string
	resource *tracing.Resource
	// muは、同じwriterへの書き込みと、Closeを直列にします。
	mu     sync.Mutex
	writer io.Writer
	closer io.Closer
}

// Newは、writerへ書くExporterを返します。closerがnilでなければ、Closeで閉じます。
func New(name string, writer io.Writer, closer io.Closer, resource *tracing.Resource) *Exporter {
	return &Exporter{name: name, resource: resource, writer: writer, closer: closer}
}

// NewStdoutは、標準出力へ書くExporterを返します。標準出力は閉じません。
func NewStdout(resource *tracing.Resource) *Exporter {
	return New(StdoutExporterName, os.Stdout, nil, resource)
}

// OpenFileは、pathのfileへ追記するExporterを返します。ないdirectoryは作ります。
// fileはrotationしません。
func OpenFile(path string, resource *tracing.Resource) (*Exporter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create the directory of the trace file: %w", err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open the trace file: %w", err)
	}
	return New(FileExporterName, file, file, resource), nil
}

func (e *Exporter) Name() string { return e.name }

// Exportは、spansをOTLP/JSONの1行にして、1回のWriteで書きます。
func (e *Exporter) Export(_ context.Context, spans []tracing.SpanData) error {
	line, err := otlp.MarshalJSON(spans, e.resource)
	if err != nil {
		return fmt.Errorf("encode spans as OTLP JSON: %w", err)
	}
	line = append(line, '\n')
	e.mu.Lock()
	defer e.mu.Unlock()
	_, err = e.writer.Write(line)
	return err
}

func (e *Exporter) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closer == nil {
		return nil
	}
	return e.closer.Close()
}
