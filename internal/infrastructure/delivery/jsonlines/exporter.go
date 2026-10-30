// Package jsonlinesは、spanやmetricsのbatchをOTLP/JSONの1行にして書く送り先（fileと
// stdout）です。送るものの種類による違いは、batchを1行にする関数で渡します。
package jsonlines

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// metricsのexporter属性の値です。
const (
	FileExporterName   = "file"
	StdoutExporterName = "stdout"
)

// Marshalは、batchを、改行を含まないOTLP/JSONにします。
type Marshal[T any] func(items []T) ([]byte, error)

// Exporterは、delivery.Exporterの実装です。
type Exporter[T any] struct {
	name    string
	marshal Marshal[T]
	// muは、同じwriterへの書き込みと、Closeを直列にします。
	mu     sync.Mutex
	writer io.Writer
	closer io.Closer
}

// Newは、writerへ書くExporterを返します。closerがnilでなければ、Closeで閉じます。
func New[T any](name string, writer io.Writer, closer io.Closer, marshal Marshal[T]) *Exporter[T] {
	return &Exporter[T]{name: name, marshal: marshal, writer: writer, closer: closer}
}

// NewStdoutは、標準出力へ書くExporterを返します。標準出力は閉じません。
func NewStdout[T any](marshal Marshal[T]) *Exporter[T] {
	return New(StdoutExporterName, os.Stdout, nil, marshal)
}

// OpenFileは、pathのfileへ追記するExporterを返します。ないdirectoryは作ります。
// fileはrotationしません。
func OpenFile[T any](path string, marshal Marshal[T]) (*Exporter[T], error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create the directory of %s: %w", path, err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	return New(FileExporterName, file, file, marshal), nil
}

func (e *Exporter[T]) Name() string { return e.name }

// Exportは、itemsをOTLP/JSONの1行にして、1回のWriteで書きます。
func (e *Exporter[T]) Export(_ context.Context, items []T) error {
	line, err := e.marshal(items)
	if err != nil {
		return fmt.Errorf("encode as OTLP JSON: %w", err)
	}
	line = append(line, '\n')
	e.mu.Lock()
	defer e.mu.Unlock()
	_, err = e.writer.Write(line)
	return err
}

func (e *Exporter[T]) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closer == nil {
		return nil
	}
	return e.closer.Close()
}
