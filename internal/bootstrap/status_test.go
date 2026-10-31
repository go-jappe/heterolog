package bootstrap_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/status"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// 組み立てたloggerは、statusの報告の材料として、出力先の一覧と、fileのdirectory、記録した
// metricsを返します。
func TestLoggerStatusListsOutputsDirectoriesAndMetrics(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()
	cfg := bootstrap.DefaultConfig()
	cfg.File = &pkglogger.FileLoggerConfig{
		LifecycleMode: fsstore.FileLifecycleModeDisable,
		RuntimeConfig: pkglogger.RuntimeConfig{LogLevel: pkglogger.LogLevelInfo},
		TimeConfig:    pkglogger.TimeConfig{TimeZone: "UTC"},
		LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano},
		FsstoreConfig: fsstore.Config{
			Layout: fsstore.FsstoreConfig{
				LogPath: root, Target: "status-test", LogDir: "logs",
				FileNameFormat: fsstore.FileNameFormat, FileDateFormat: fsstore.FileDateFormat,
			},
			Lifecycle: fsstore.FileLifecycleConfig{MaxFileSize: 1 << 20, MaxFileBackups: 2, MaxFileAge: 1, MaxDirSize: 10 << 20},
			Retry:     fsstore.RetryConfig{MaxWriteRetries: 0},
			Overload:  fsstore.FileArchiveOverloadPolicyConfig{OverloadPolicy: fsstore.OverloadPolicyDrop, MinAvailableDiskSpace: 1 << 20},
		},
	}
	cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile, pkglogger.LogOutputStdout}
	logger, err := bootstrap.New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = logger.Close() }()
	if err := logger.Info(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	got := logger.Status(ctx)
	if got.Kind != "logger" || len(got.Inspection.Backends) != 2 ||
		got.Inspection.Backends[0].Name != "file" || got.Inspection.Backends[1].Name != "stdout" || got.Inspection.Backends[0].Spool != nil {
		t.Errorf("status = %+v, want the file and stdout outputs without spools", got.Inspection.Backends)
	}
	want := status.DirectoryInspection{Path: filepath.Join(root, "status-test", "logs"), Role: "log", Backend: "file", LimitBytes: 10 << 20, MinFreeBytes: 1 << 20}
	if len(got.Inspection.Directories) != 1 || got.Inspection.Directories[0] != want {
		t.Errorf("directories = %+v, want %+v", got.Inspection.Directories, want)
	}
	if len(got.Metrics) == 0 {
		t.Errorf("metrics are empty after a write")
	}
}

// 組み立てたtelemetryは、送り先ごとに、failure storeのspoolの場所と送り直しの状態を返します。
func TestTelemetryStatusListsDestinationsWithTheirSpools(t *testing.T) {
	spoolDir := t.TempDir()
	ctx := context.Background()
	cfg := spoolTelemetryConfig("http://127.0.0.1:1", spoolDir)
	tel, err := bootstrap.NewTelemetry(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tel.Shutdown(ctx) }()
	got := tel.Status(ctx)
	if got.Kind != "telemetry" || len(got.Inspection.Backends) != 1 {
		t.Fatalf("status = %+v, want one backend", got.Inspection.Backends)
	}
	backend := got.Inspection.Backends[0]
	if backend.Name != "otlp_traces" || backend.Exporter != "otlp" || backend.Signal != status.SignalTraces || backend.Spool == nil {
		t.Fatalf("backend = %+v, want otlp_traces with a spool", backend)
	}
	spool := backend.Spool
	wantRoot := filepath.Join(spoolDir, "checkout", "spool", "traces_otlp")
	if spool.Root != wantRoot || spool.StateDirs["active"] != filepath.Join(wantRoot, "active") || len(spool.StateDirs) != 5 ||
		spool.ActiveLimitBytes != 1<<30 {
		t.Errorf("spool = %+v, want the spool under %s", spool, wantRoot)
	}
	// 送り直すrunnerは背景で始まるため、動き出すまで待ちます。
	deadline := time.Now().Add(5 * time.Second)
	for spool.Replay.State != "running" {
		if time.Now().After(deadline) {
			t.Fatalf("replay state = %q, want running", spool.Replay.State)
		}
		time.Sleep(10 * time.Millisecond)
		spool = tel.Status(ctx).Inspection.Backends[0].Spool
	}
}
