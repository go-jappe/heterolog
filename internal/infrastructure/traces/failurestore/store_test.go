package failurestore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func archiveConfig(root string) Config {
	return Config{Store: pkgdurable.FailureStoreConfig{
		MaxConcurrent: 1,
		DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive},
		DurableStoreConfig: pkgdurable.DurableStoreConfig{FileArchiveConfig: pkgdurable.FileArchiveConfig{
			Target:                     "checkout",
			MaxFileSize:                1024 * 1024,
			FailureFileArchivePath:     root,
			Dir:                        "traces_otlp_failure_archives",
			DirCaseRetryable:           "retryable",
			DirCasePermanent:           "permanent",
			FileNameFormat:             pkgfsstore.FailureArchiveFileNameFormat,
			FileDateFormat:             pkgfsstore.FileDateFormat,
			UsageCacheTTL:              time.Hour,
			CompressFormat:             pkgfsstore.CompressGzip,
			RotationType:               pkgfsstore.FileRotationDaily,
			FileOperationRetryWaitTime: time.Millisecond,
			OverloadPolicyConfig: pkgdurable.FileArchiveOverloadPolicyConfig{
				OverloadPolicy:        pkgdurable.OverloadPolicyDrop,
				MinAvailableDiskSpace: pkgdurable.MinFileArchiveAvailableDiskSpace,
			},
		}},
	}}
}

func TestStoreArchivesEachSpanWithItsDisposition(t *testing.T) {
	root := t.TempDir()
	store, err := New(context.Background(), archiveConfig(root), Options{
		Backend: "otlp_traces", Exporter: &recordingExporter{}, Resource: testResource, Report: func(err error) { t.Error(err) },
	})
	if err != nil || store == nil {
		t.Fatalf("New = %v, %v", store, err)
	}
	batch := []tracing.SpanData{
		{TraceID: tracing.TraceID{1}, SpanID: tracing.SpanID{1}, Name: "retry-span"},
		{TraceID: tracing.TraceID{1}, SpanID: tracing.SpanID{2}, Name: "dead-span"},
		{TraceID: tracing.TraceID{1}, SpanID: tracing.SpanID{3}, Name: "sent-span"},
	}
	err = store.Store(context.Background(), batch, []traces.SpanFailure{
		{Start: 0, End: 1, Disposition: traces.DispositionRetry, Reason: "http_status", StatusCode: 503, Err: errors.New("unavailable")},
		{Start: 1, End: 2, Disposition: traces.DispositionDead, Reason: "http_status", StatusCode: 400, Err: errors.New("bad request")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	contents := map[string]string{}
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".jsonl") {
			return err
		}
		data, err := os.ReadFile(path)
		contents[filepath.Base(filepath.Dir(path))] += string(data)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	// 送り直す扱いのspanはretryableへ、拒否されたspanはpermanentへ、OTLP/JSONで残します。
	retryable, permanent := contents["retryable"], contents["permanent"]
	if !strings.Contains(retryable, `"name":"retry-span"`) || !strings.Contains(retryable, `"backend":"otlp_traces"`) ||
		!strings.Contains(retryable, `"failure_code":"http_status"`) || strings.Contains(retryable, "dead-span") {
		t.Errorf("retryable archive = %s", retryable)
	}
	if !strings.Contains(permanent, `"name":"dead-span"`) || !strings.Contains(permanent, `"failure_category":"dead"`) ||
		!strings.Contains(permanent, `"status_code":400`) || strings.Contains(permanent, "sent-span") {
		t.Errorf("permanent archive = %s", permanent)
	}
}

func TestNewReturnsNilWithoutAStore(t *testing.T) {
	store, err := New(context.Background(), Config{Store: pkgdurable.FailureStoreConfig{
		DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
	}}, Options{Backend: "otlp_traces"})
	if err != nil || store != nil {
		t.Errorf("New = %v, %v; want no store", store, err)
	}
	// nilのStoreを止めても何もしません。
	if err := store.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown of nil = %v", err)
	}
}

func TestInternalLogsReachTheErrorHandler(t *testing.T) {
	var mu sync.Mutex
	var reported []string
	handlers := internalHandlers(func(err error) {
		mu.Lock()
		defer mu.Unlock()
		reported = append(reported, err.Error())
	})(pkgdurable.DurableTypeSpool, nil)
	handlers.OnError(context.Background(), errors.New("disk full"), &logging.LogEntry{
		Source: "failure_spool", Message: "failed to store", Fields: map[string]any{"b": 2, "a": 1},
	}, nil, nil)
	handlers.OnWarn(context.Background(), &logging.LogEntry{Source: "failure_spool", Message: "backlog"}, nil, nil)
	if handlers.OnInfo != nil || handlers.OnDebug != nil {
		t.Error("INFO and DEBUG are reported, want only ERROR and WARN")
	}
	want := []string{
		"trace failure store failure_spool: failed to store a=1 b=2: disk full",
		"trace failure store failure_spool: backlog",
	}
	if len(reported) != 2 || reported[0] != want[0] || reported[1] != want[1] {
		t.Errorf("reported = %q, want %q", reported, want)
	}
}
