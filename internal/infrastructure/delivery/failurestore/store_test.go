package failurestore

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/storageowner"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
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

func TestStoreArchivesEachItemWithItsDisposition(t *testing.T) {
	root := t.TempDir()
	store, err := New(context.Background(), archiveConfig(root), Options[string]{
		Backend: "otlp_traces", Operation: "export_traces", Exporter: &recordingExporter{}, Codec: testCodec, Report: func(err error) { t.Error(err) },
	})
	if err != nil || store == nil {
		t.Fatalf("New = %v, %v", store, err)
	}
	batch := []string{"retry-span", "dead-span", "sent-span"}
	err = store.Store(context.Background(), batch, []delivery.Failure{
		{Start: 0, End: 1, Disposition: delivery.DispositionRetry, Reason: "http_status", StatusCode: 503, Err: errors.New("unavailable")},
		{Start: 1, End: 2, Disposition: delivery.DispositionDead, Reason: "http_status", StatusCode: 400, Err: errors.New("bad request")},
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
	// 送り直す扱いのものはretryableへ、拒否されたものはpermanentへ、Codecのrecordで残します。
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
	}}, Options[string]{Backend: "otlp_traces"})
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
		"telemetry failure store failure_spool: failed to store a=1 b=2: disk full",
		"telemetry failure store failure_spool: backlog",
	}
	if len(reported) != 2 || reported[0] != want[0] || reported[1] != want[1] {
		t.Errorf("reported = %q, want %q", reported, want)
	}
}

// fakeObjectStorageは、受け取ったPUTのpathを記録するS3互換の受信側です。
type fakeObjectStorage struct {
	server *httptest.Server
	mu     sync.Mutex
	paths  []string
}

func newFakeObjectStorage(t *testing.T) *fakeObjectStorage {
	t.Helper()
	storage := &fakeObjectStorage{}
	storage.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		storage.mu.Lock()
		storage.paths = append(storage.paths, r.URL.Path)
		storage.mu.Unlock()
		w.Header().Set("ETag", `"test-etag"`)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(storage.server.Close)
	return storage
}

func (s *fakeObjectStorage) uploaded(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, path := range s.paths {
		if strings.HasSuffix(path, "/"+name) {
			return true
		}
	}
	return false
}

func TestStoreUploadsTheArchiveWithItsOwnUploadStorage(t *testing.T) {
	root := t.TempDir()
	spoolRoot := t.TempDir()
	storage := newFakeObjectStorage(t)
	cfg := archiveConfig(root)
	archive := &cfg.Store.DurableStoreConfig.FileArchiveConfig
	archive.UsageCacheTTL = time.Millisecond
	archive.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	archive.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{
		Directory: filepath.Join(root, "checkpoints"), SourcePrefix: "telemetry_otlp_traces_failure_archive", AutoRecoverMissingCheckpoint: true,
	}
	archive.ObjectStorageUploadConfig = objectstorage.UploadTargetConfig{
		Enabled: true, Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		S3: objectstorage.S3Config{
			Region: "us-east-1", Bucket: "spans", Prefix: "traces", Endpoint: storage.server.URL,
			ForcePathStyle: true, AccessKeyID: "test", SecretAccessKey: "test", Timeout: time.Second,
		},
		// 設定がloggerの契約を指していても、Telemetryの保存先を使います。
		CompressedLogFileUploadFailureStore: objectstorage.CompressedLogFileUploadFailureStoreConfig{
			DurableType: []failurestore.DurableType{failurestore.DurableTypeSpool},
		},
		CompressedLogFileUploadSpool: objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(objectstorage.CompressedLogFileUploadSpoolConfig{
			SpoolPath: spoolRoot, Target: "checkout", LoggerContract: pkglogger.LoggerContractSync.String(),
			RunnerInterval: time.Hour, LifecycleInterval: time.Hour, CircuitBreakerOpenCooldown: time.Second, MaxRetryAfter: time.Hour,
		}),
	}
	store, err := New(context.Background(), cfg, Options[string]{
		Backend: "otlp_traces", Operation: "export_traces", Exporter: &recordingExporter{}, Codec: testCodec, Report: func(err error) { t.Error(err) },
	})
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	t.Cleanup(func() { _ = store.Shutdown(context.Background()) })
	// 前の日に圧縮した世代を置き、最初の保存の後のcleanupで送らせます。
	permanent := filepath.Join(root, "checkout", "traces_otlp_failure_archives", "permanent")
	if err := os.MkdirAll(permanent, 0o750); err != nil {
		t.Fatal(err)
	}
	const previous = "checkout_20260902_1.jsonl.gz"
	if err := os.WriteFile(filepath.Join(permanent, previous), gzipped(t, "{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	dead := []delivery.Failure{{Start: 0, End: 1, Disposition: delivery.DispositionDead, Err: errors.New("bad request")}}
	batch := []string{"dead-span"}
	if err := store.Store(context.Background(), batch, dead); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !storage.uploaded(previous) {
		if time.Now().After(deadline) {
			t.Fatalf("the compressed archive was not uploaded; uploads %v", storage.paths)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := store.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	// uploadできなかった圧縮fileのspoolは、loggerではなくTelemetryの場所に作ります。
	owners, err := filepath.Glob(filepath.Join(spoolRoot, "checkout", "spool", "*", "*", "*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(owners) != 1 || filepath.Base(owners[0]) != storageowner.Telemetry.String() {
		t.Errorf("upload spool directories = %v, want one owned by the telemetry", owners)
	}
}

func gzipped(t *testing.T, content string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
