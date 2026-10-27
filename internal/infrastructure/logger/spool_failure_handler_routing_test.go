package logger

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// factoryで構築したspool handlerが、spool用HandlerConfigで選択された通知先だけを
// 内部error/warnの委譲先として使うことを実処理で確認する。
func TestBuildRoutesSpoolInternalNotificationsOnlyToConfiguredHandlers(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	clk := spoolRoutingFixedClock{now: now}
	root := t.TempDir()

	cfg := loggerdurable.NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
		MaxConcurrent: 1,
		DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeSpool},
		DurableStoreConfig: pkgdurable.DurableStoreConfig{
			SpoolConfig: pkgdurable.SpoolConfig{
				SpoolPath: root,
				Target:    "routing-test",
				Dir:       "backend",
				OverloadPolicyConfig: pkgdurable.SpoolOverloadPolicyConfig{
					OverloadPolicy:             pkgdurable.OverloadPolicyDrop,
					ActiveBacklogWarnAfterDays: 1,
				},
				HandlerConfig: pkgloggerhandler.HandlerConfig{
					OnErrorConfig: []string{" stdout ", "unknown", "stdout"},
					OnWarnConfig:  []string{"slack"},
				},
			},
		},
	})

	var mu sync.Mutex
	calls := make(map[string]int)
	record := func(name string) {
		mu.Lock()
		defer mu.Unlock()
		calls[name]++
	}
	available := handlerStruct{
		OnError: []logging.NamedLoggerErrorHandler{
			{Name: "file", Handler: func(context.Context, error, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {
				record("error:file")
			}},
			{Name: "stdout", Handler: func(context.Context, error, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {
				record("error:stdout")
			}},
			{Name: "slack", Handler: func(context.Context, error, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {
				record("error:slack")
			}},
		},
		OnWarn: []logging.NamedLoggerHandler{
			{Name: "file", Handler: func(context.Context, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) { record("warn:file") }},
			{Name: "stdout", Handler: func(context.Context, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) { record("warn:stdout") }},
			{Name: "slack", Handler: func(context.Context, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) { record("warn:slack") }},
		},
	}

	composite, err := flushfailure.Build(
		context.Background(),
		clk,
		cfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		flushfailure.FactoryOptions{
			InternalLogHandlerFactory: failureStoreInternalLogHandlerFactory("test-backend", func(string) handlerStruct {
				return available
			}),
		},
	)
	if err != nil {
		t.Fatalf("Build error = %v", err)
	}
	if len(composite.Handlers) != 1 {
		t.Fatalf("handler count = %d, want 1", len(composite.Handlers))
	}
	spoolHandler, ok := composite.Handlers[0].(*spoolstorehandler.SpoolStoreHandler)
	if !ok {
		t.Fatalf("handler type = %T, want *spoolstorehandler.SpoolStoreHandler", composite.Handlers[0])
	}

	activeDir := spoolHandler.Store.RootDir.ActiveDir().Path()
	if err := os.MkdirAll(activeDir, 0o755); err != nil {
		t.Fatalf("create active dir: %v", err)
	}
	stalePath := filepath.Join(activeDir, "stale.hlogspool")
	if err := os.WriteFile(stalePath, []byte("stale"), 0o600); err != nil {
		t.Fatalf("write stale spool: %v", err)
	}
	staleAt := now.Add(-48 * time.Hour)
	if err := os.Chtimes(stalePath, staleAt, staleAt); err != nil {
		t.Fatalf("set stale spool time: %v", err)
	}

	// 古いactive spoolがwarnを、payload欠落がerrorを発生させる。
	// どちらもfactoryが配線したspool自身の内部通知経路を通る。
	composite.HandleFailure(context.Background(), loggerfailure.SendResult{
		Meta: loggerfailure.Meta{
			Backend:     "test-backend",
			RequestID:   "routing-test",
			AttemptedAt: now,
			InputCount:  1,
		},
		RetryableRecords: []loggerfailure.FailedRecord{{FailedAt: now}},
	})

	want := map[string]int{
		"error:stdout": 1,
		"warn:slack":   1,
	}
	// Drainは受理済みの処理と、そこから始めたdirectory scanの終了を待ちます。通知先は
	// 同期的に呼ばれるため、Drainの完了後の記録が最終の件数です。期限は停止の検出用です。
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelDrain()
	if err := composite.Drain(drainCtx); err != nil {
		t.Fatalf("Drain() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != len(want) {
		t.Fatalf("notification calls = %#v, want %#v", calls, want)
	}
	for name, count := range want {
		if calls[name] != count {
			t.Errorf("notification calls[%q] = %d, want %d; all=%#v", name, calls[name], count, calls)
		}
	}
}

type spoolRoutingFixedClock struct {
	now time.Time
}

func (c spoolRoutingFixedClock) Now() time.Time { return c.now }
