package evacuation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"testing/synctest"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestNewHandlerBuildsStdoutDestination(t *testing.T) {
	handler, err := NewHandler(pkgdurable.SpillDestinationStdout)
	if err != nil {
		t.Fatalf("NewHandler error = %v", err)
	}
	if handler == nil {
		t.Fatal("NewHandler = nil, want stdout handler")
	}
}

func TestNewHandlerRejectsUnsupportedDestinations(t *testing.T) {
	for _, destination := range []pkgdurable.SpillDestination{
		pkgdurable.SpillDestinationS3,
		pkgdurable.SpillDestinationNFS,
		pkgdurable.SpillDestination("unknown"),
		"",
	} {
		t.Run(string(destination), func(t *testing.T) {
			handler, err := NewHandler(destination)
			if !errors.Is(err, ErrUnsupportedSpillDestination) {
				t.Fatalf("NewHandler error = %v, want ErrUnsupportedSpillDestination", err)
			}
			if handler != nil {
				t.Fatalf("NewHandler = %T, want nil", handler)
			}
			var unsupported *UnsupportedSpillDestinationError
			if !errors.As(err, &unsupported) || unsupported.Destination != destination {
				t.Fatalf("NewHandler error = %v, want UnsupportedSpillDestinationError for %q", err, destination)
			}
		})
	}
}

// 退避先の設定誤りはロガーの構築errorとして利用者が読みます。指定した退避先と実装済みの退避先を
// 文面だけで判別できるよう、書式をこの試験で固定します。他の試験はerrors.Asでfieldを確認します。
func TestUnsupportedSpillDestinationErrorFormat(t *testing.T) {
	for _, tt := range []struct {
		destination pkgdurable.SpillDestination
		want        string
	}{
		{destination: pkgdurable.SpillDestinationS3, want: `unsupported spill destination: "s3"; only "stdout" is implemented`},
		{destination: "", want: `unsupported spill destination: ""; only "stdout" is implemented`},
	} {
		err := &UnsupportedSpillDestinationError{Destination: tt.destination}
		if got := err.Error(); got != tt.want {
			t.Fatalf("Error() = %q, want %q", got, tt.want)
		}
	}
}

func TestStdoutHandlersKeepEachSendResultContiguousAcrossInstances(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writer := newBlockingMetaWriter(t)
		firstHandler := newStdoutHandler(writer)
		secondHandler := newStdoutHandler(writer)

		firstResult := loggerfailure.SendResult{
			Meta: loggerfailure.Meta{
				Backend:    "opensearch",
				RequestID:  "request-a",
				TraceID:    "trace-a",
				InputCount: 2,
			},
			DeadRecords: []loggerfailure.FailedRecord{{
				Operation: "index",
				Payload:   map[string]any{"message": "permanent-a"},
			}},
			RetryableRecords: []loggerfailure.FailedRecord{{
				Operation: "index",
				Payload:   map[string]any{"message": "retryable-a"},
			}},
		}
		secondResult := loggerfailure.SendResult{
			Meta: loggerfailure.Meta{
				Backend:    "loki",
				RequestID:  "request-b",
				TraceID:    "trace-b",
				InputCount: 1,
			},
			RetryableRecords: []loggerfailure.FailedRecord{{
				Operation: "push",
				Payload:   map[string]any{"message": "retryable-b"},
			}},
		}

		done := make(chan error, 2)
		go func() {
			done <- firstHandler.Handle(context.Background(), firstResult)
		}()

		select {
		case <-writer.firstMetaWritten:
		case err := <-done:
			t.Fatalf("first handler returned before writing its meta row: %v", err)
		}

		go func() {
			done <- secondHandler.Handle(context.Background(), secondResult)
		}()

		// The first writer call is intentionally blocked. A mutex scoped to one
		// handler instance would allow the second meta row through here; the
		// package-wide batch mutex must keep it out until the first batch finishes.
		// Once every goroutine is blocked, the second handler can only be waiting
		// for that batch mutex.
		synctest.Wait()
		select {
		case <-writer.secondMetaWritten:
			t.Fatal("second handler wrote before the first SendResult batch completed")
		default:
		}
		writer.releaseFirst()

		for range 2 {
			if err := <-done; err != nil {
				t.Fatalf("stdout evacuation error = %v", err)
			}
		}

		rows := decodeOutputRows(t, writer.Bytes())
		wantRequestIDs := []string{
			"request-a",
			"request-a",
			"request-a",
			"request-b",
			"request-b",
		}
		if len(rows) != len(wantRequestIDs) {
			t.Fatalf("output rows = %d, want %d; output=%s", len(rows), len(wantRequestIDs), writer.Bytes())
		}

		for i, row := range rows {
			if got := stringField(t, row, "requestID"); got != wantRequestIDs[i] {
				t.Fatalf("row[%d] requestID = %q, want %q; rows=%v", i, got, wantRequestIDs[i], rows)
			}
			if stringField(t, row, "type") != "record" {
				continue
			}

			wantBackend := "opensearch"
			wantTraceID := "trace-a"
			if wantRequestIDs[i] == "request-b" {
				wantBackend = "loki"
				wantTraceID = "trace-b"
			}
			if got := stringField(t, row, "backend"); got != wantBackend {
				t.Fatalf("row[%d] backend = %q, want %q", i, got, wantBackend)
			}
			if got := stringField(t, row, "traceID"); got != wantTraceID {
				t.Fatalf("row[%d] traceID = %q, want %q", i, got, wantTraceID)
			}
		}
	})
}

type blockingMetaWriter struct {
	mu sync.Mutex

	buffer bytes.Buffer

	firstMetaWritten  chan struct{}
	secondMetaWritten chan struct{}
	releaseFirstMeta  chan struct{}
	firstMetaOnce     sync.Once
	secondMetaOnce    sync.Once
	releaseOnce       sync.Once
}

// newBlockingMetaWriter returns a writer that blocks the first meta row until releaseFirst.
// The release is registered with t.Cleanup so a failing assertion cannot leave the write blocked.
func newBlockingMetaWriter(t *testing.T) *blockingMetaWriter {
	t.Helper()
	writer := &blockingMetaWriter{
		firstMetaWritten:  make(chan struct{}),
		secondMetaWritten: make(chan struct{}),
		releaseFirstMeta:  make(chan struct{}),
	}
	t.Cleanup(writer.releaseFirst)
	return writer
}

// releaseFirst lets the blocked first meta row finish. It may be called more than once.
func (w *blockingMetaWriter) releaseFirst() {
	w.releaseOnce.Do(func() { close(w.releaseFirstMeta) })
}

func (w *blockingMetaWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	_, err := w.buffer.Write(p)
	w.mu.Unlock()
	if err != nil {
		return 0, err
	}

	isMeta := bytes.Contains(p, []byte(`"type":"meta"`))
	switch {
	case isMeta && bytes.Contains(p, []byte(`"requestID":"request-a"`)):
		w.firstMetaOnce.Do(func() { close(w.firstMetaWritten) })
		<-w.releaseFirstMeta
	case isMeta && bytes.Contains(p, []byte(`"requestID":"request-b"`)):
		w.secondMetaOnce.Do(func() { close(w.secondMetaWritten) })
	}

	return len(p), nil
}

func (w *blockingMetaWriter) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()

	return bytes.Clone(w.buffer.Bytes())
}

func decodeOutputRows(t *testing.T, output []byte) []map[string]any {
	t.Helper()

	lines := bytes.Split(bytes.TrimSpace(output), []byte{'\n'})
	rows := make([]map[string]any, 0, len(lines))
	for i, line := range lines {
		var row map[string]any
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatalf("decode output row[%d]: %v; row=%s", i, err, line)
		}
		rows = append(rows, row)
	}
	return rows
}

func stringField(t *testing.T, row map[string]any, key string) string {
	t.Helper()

	value, ok := row[key].(string)
	if !ok {
		t.Fatalf("row field %q = %#v, want string; row=%v", key, row[key], row)
	}
	return value
}
