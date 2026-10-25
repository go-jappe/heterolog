package appendlog

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// A negative limit means that this attempt accepts the whole remaining input.
// Otherwise the writer appends only the reported prefix, just as a partial file
// write does. allowShortNil is reserved for explicitly malformed-writer cases.
type scriptedStringWriteResult struct {
	limit         int
	err           error
	allowShortNil bool
}

type scriptedStringWriter struct {
	results  []scriptedStringWriteResult
	requests []string
	contents bytes.Buffer
}

func (w *scriptedStringWriter) WriteString(input string) (int, error) {
	if len(w.requests) >= len(w.results) {
		panic("scriptedStringWriter received more writes than configured")
	}
	result := w.results[len(w.requests)]
	w.requests = append(w.requests, input)
	n := result.limit
	if n < 0 {
		n = len(input)
	}
	if n > len(input) {
		panic("scriptedStringWriter cannot write more bytes than it received")
	}
	if n < len(input) && result.err == nil && !result.allowShortNil {
		panic("ordinary scripted writes must obey the io.Writer short-write contract")
	}
	_, _ = w.contents.WriteString(input[:n])
	return n, result.err
}

func TestWriteStringWithRetryPreservesOneRecordAcrossPartialWrites(t *testing.T) {
	writeFailure := errors.New("injected write failure")
	for _, test := range []struct {
		name         string
		message      string
		attempts     []scriptedStringWriteResult
		maxAttempts  int
		wantRequests []string
		wantContents string
		wantErr      error
	}{
		{
			name: "complete first attempt", message: "message",
			attempts:     []scriptedStringWriteResult{{limit: -1}},
			wantRequests: []string{"message\n"}, wantContents: "message\n",
		},
		{
			name: "partial failure followed by success", message: "message",
			attempts: []scriptedStringWriteResult{
				{limit: 2, err: writeFailure}, {limit: -1},
			},
			wantRequests: []string{"message\n", "ssage\n"}, wantContents: "message\n",
		},
		{
			name: "several partial failures followed by success", message: "message",
			attempts: []scriptedStringWriteResult{
				{limit: 2, err: writeFailure}, {limit: 3, err: writeFailure}, {limit: -1},
			},
			wantRequests: []string{"message\n", "ssage\n", "ge\n"}, wantContents: "message\n",
		},
		{
			name: "exhausted retries report actual partial bytes", message: "message",
			attempts: []scriptedStringWriteResult{
				{limit: 2, err: writeFailure}, {limit: 1, err: writeFailure}, {limit: 1, err: writeFailure},
			},
			wantRequests: []string{"message\n", "ssage\n", "sage\n"}, wantContents: "mess",
			wantErr: writeFailure,
		},
		{
			name: "failure without progress retries the full record", message: "message",
			attempts: []scriptedStringWriteResult{
				{limit: 0, err: writeFailure}, {limit: -1},
			},
			wantRequests: []string{"message\n", "message\n"}, wantContents: "message\n",
		},
		{
			name: "no progress and exhausted retries", message: "message",
			attempts: []scriptedStringWriteResult{
				{limit: 0, err: writeFailure}, {limit: 0, err: writeFailure},
			},
			wantRequests: []string{"message\n", "message\n"}, wantErr: writeFailure,
		},
		{
			name: "newline is retried without repeating the message", message: "message",
			attempts: []scriptedStringWriteResult{
				{limit: 7, err: writeFailure}, {limit: -1},
			},
			wantRequests: []string{"message\n", "\n"}, wantContents: "message\n",
		},
		{
			name: "partial UTF-8 character resumes at byte boundary", message: "日本",
			attempts: []scriptedStringWriteResult{
				{limit: 1, err: writeFailure}, {limit: -1},
			},
			wantRequests: []string{"日本\n", "日本\n"[1:]}, wantContents: "日本\n",
		},
		{
			name: "all bytes with an error are not written again", message: "message", maxAttempts: 3,
			attempts:     []scriptedStringWriteResult{{limit: -1, err: writeFailure}},
			wantRequests: []string{"message\n"}, wantContents: "message\n", wantErr: writeFailure,
		},
		{
			name: "partial then all bytes with an error", message: "message", maxAttempts: 3,
			attempts: []scriptedStringWriteResult{
				{limit: 2, err: writeFailure}, {limit: -1, err: writeFailure},
			},
			wantRequests: []string{"message\n", "ssage\n"}, wantContents: "message\n", wantErr: writeFailure,
		},
		{
			name: "defensive retry for a writer returning a short count with nil", message: "message",
			attempts: []scriptedStringWriteResult{
				{limit: 2, allowShortNil: true}, {limit: -1},
			},
			wantRequests: []string{"message\n", "ssage\n"}, wantContents: "message\n",
		},
		{
			name: "short count with nil cannot report success when attempts run out", message: "message",
			attempts: []scriptedStringWriteResult{
				{limit: 2, allowShortNil: true}, {limit: 1, allowShortNil: true},
			},
			wantRequests: []string{"message\n", "ssage\n"}, wantContents: "mes", wantErr: io.ErrShortWrite,
		},
		{
			name: "empty message still writes one newline", message: "",
			attempts:     []scriptedStringWriteResult{{limit: -1}},
			wantRequests: []string{"\n"}, wantContents: "\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newPartialWriteTestStore(t)
			writer := &scriptedStringWriter{results: test.attempts}
			attempts := test.maxAttempts
			if attempts == 0 {
				attempts = len(test.attempts)
			}
			latencyCalls := 0
			written, err := store.executeWriteStringWithRetry(
				context.Background(), "test.appendlog.partial-write", test.message, "request-id",
				nil, nil, "write failed",
				func(_ context.Context, _ time.Duration, meta any) {
					latencyCalls++
					if meta != "request-id" {
						t.Errorf("latency metadata = %v, want request-id", meta)
					}
					if writer.contents.String() != test.message+"\n" {
						t.Error("reported successful write latency before the complete record was written")
					}
				},
				NewRetryConfigAdapter(pkgfsstore.RetryConfig{MaxWriteAttempts: attempts}), writer,
			)
			if test.wantErr == nil {
				if err != nil {
					t.Fatalf("write error = %v", err)
				}
			} else if !errors.Is(err, test.wantErr) {
				t.Fatalf("write error = %v, want cause %v", err, test.wantErr)
			}
			if got := writer.contents.String(); got != test.wantContents {
				t.Fatalf("written content = %q, want %q", got, test.wantContents)
			}
			if written != uint64(len(test.wantContents)) {
				t.Fatalf("written bytes = %d, want actual appended length %d", written, len(test.wantContents))
			}
			if !reflect.DeepEqual(writer.requests, test.wantRequests) {
				t.Fatalf("write requests = %q, want %q", writer.requests, test.wantRequests)
			}
			wantLatencyCalls := 1
			if test.wantErr != nil {
				wantLatencyCalls = 0
			}
			if latencyCalls != wantLatencyCalls {
				t.Fatalf("successful latency notifications = %d, want %d", latencyCalls, wantLatencyCalls)
			}
		})
	}
}

func TestWriteStringWithRetryAppendsOneRecordToExistingFile(t *testing.T) {
	store := newPartialWriteTestStore(t)
	if _, err := store.File.WriteString("previous record\n"); err != nil {
		t.Fatal(err)
	}
	written, err := store.WriteStringWithRetry(
		context.Background(), "test.appendlog.file-write", "日本", nil, nil, nil, "write failed", nil,
		NewRetryConfigAdapter(pkgfsstore.RetryConfig{MaxWriteAttempts: 2}),
	)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(store.File.Name())
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "previous record\n日本\n" || written != uint64(len("日本\n")) {
		t.Fatalf("file content = %q, byte count = %d", contents, written)
	}
}

func newPartialWriteTestStore(t *testing.T) *FileStore {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "rewrite-*.log")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return &FileStore{File: file, Tracer: observer.NopTracer{}}
}

// active fileを開いていない書き込みは、再試行を使い切った後もEnsureWritableErrorと
// その原因をerrors.As・errors.Isで判定できる形で返します。
func TestWriteStringWithRetryWithoutActiveFileKeepsEnsureWritableCause(t *testing.T) {
	store := &FileStore{Tracer: observer.NopTracer{}}
	_, err := store.WriteStringWithRetry(
		context.Background(), "test.appendlog.closed-file", "message", nil, nil, nil, "write failed", nil,
		NewRetryConfigAdapter(pkgfsstore.RetryConfig{MaxWriteAttempts: 2}),
	)
	var writableErr *EnsureWritableError
	if !errors.As(err, &writableErr) {
		t.Fatalf("write error = %v, want *EnsureWritableError", err)
	}
	if !errors.Is(err, errFileClosedOrNotOpened) {
		t.Fatalf("write error = %v, want errFileClosedOrNotOpened as its cause", err)
	}
}
