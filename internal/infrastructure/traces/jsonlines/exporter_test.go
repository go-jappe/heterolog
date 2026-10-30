package jsonlines

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/tracing"
)

func TestOpenFileAppendsOneJSONLinePerBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "spans.jsonl")
	// 既存の内容は残し、後ろへ足します。
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{}\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	exporter, err := OpenFile(path, &tracing.Resource{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"first", "second"} {
		if err := exporter.Export(context.Background(), []tracing.SpanData{{TraceID: tracing.TraceID{1}, SpanID: tracing.SpanID{1}, Name: name}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := exporter.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if len(lines) != 3 || lines[0] != "{}" {
		t.Fatalf("lines = %q, want the existing line and one line per batch", lines)
	}
	var data struct {
		ResourceSpans []struct {
			ScopeSpans []struct {
				Spans []struct {
					Name string `json:"name"`
				} `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"resourceSpans"`
	}
	if err := json.Unmarshal([]byte(lines[2]), &data); err != nil {
		t.Fatal(err)
	}
	if got := data.ResourceSpans[0].ScopeSpans[0].Spans[0].Name; got != "second" {
		t.Errorf("last line span = %q, want second", got)
	}
}

func TestOpenFileReportsAnUnusablePath(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, nil, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFile(filepath.Join(blocker, "spans.jsonl"), nil); err == nil {
		t.Error("OpenFile under a regular file succeeded")
	}
}
