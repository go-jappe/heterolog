package bootstrap_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/onerrorhandler"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// selfNotificationNestingLimitは、内部ログの通知のspanの入れ子の上限です。正しい配線では
// 元の書き込みが出した通知の1段で、上限に達しません。
const selfNotificationNestingLimit = 6

type selfNotificationDepthKey struct{}

// selfNotificationTracerは、WithObserversで渡すtracerとして、内部ログの通知のspanの入れ子を
// 数えます。上限を超えた場合は記録してpanicし、内部ログの通知のfan-outが持つrecoverで再帰を
// 打ち切ります。壊れた実装でもtest binaryを落とさず、試験の失敗として報告するための安全網です。
type selfNotificationTracer struct {
	exceeded atomic.Bool
	// notificationsは、通知のspanを始めた回数です。安全網が通知を見ていることを確かめます。
	notifications atomic.Int64
}

func (tr *selfNotificationTracer) StartSpan(
	ctx context.Context,
	name string,
	attrs ...pkgobserver.Attr,
) (context.Context, pkgobserver.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	if name == onerrorhandler.SpanInternalLogNotify {
		tr.notifications.Add(1)
		depth, _ := ctx.Value(selfNotificationDepthKey{}).(int)
		depth++
		if depth > selfNotificationNestingLimit {
			tr.exceeded.Store(true)
			panic(errors.New("internal log notifications nested beyond the limit"))
		}
		ctx = context.WithValue(ctx, selfNotificationDepthKey{}, depth)
	}
	return observer.NopTracer{}.StartSpan(ctx, name, attrs...)
}

// TestSelfSelectedFileNotificationsDoNotReenterTheLoggerは、constructor（New・NewAsync）で
// fileの内部ログの通知先にfile自身を選び、全ての書き込みを閾値超過の遅延として通知させても、
// 元の記録ごとに遅延の内部ログが一行だけ同じfileへ書き込まれ、ReentryGuardが再入を一度も検出
// しないことを確認します。file通知先は内部ログを通知しない経路で書き込むため、本番の配線は
// guardに依存しません。他の出力先・条件・Delivery APIは、これらのconstructorが使う
// internal/infrastructure/loggerの試験（self_notification_*_test.go）で確認します。
// 再入の検出回数はprocess全体の値なので、t.Parallelを使いません。
func TestSelfSelectedFileNotificationsDoNotReenterTheLogger(t *testing.T) {
	const (
		events  = 3
		message = "public self-selected write"
	)
	ctx := context.Background()
	for _, kind := range []string{"sync", "async"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			cfg := directoryWhitespaceConfig(root, "self-notification", "logs")
			self := []string{pkglogger.LogOutputFile.String()}
			cfg.File.HandlerConfig = pkgloggerhandler.HandlerConfig{
				OnErrorConfig: self, OnWarnConfig: self, OnInfoConfig: self, OnDebugConfig: self,
			}
			// 負の閾値なので、時計の分解能で計測値が0でも全ての書き込みが遅延のerrorになります。
			cfg.File.FsstoreConfig.Duration.WriteFileWarnThresholdDuration = -time.Nanosecond
			cfg.File.FsstoreConfig.Duration.WriteFileErrorThresholdDuration = -time.Nanosecond
			tracer := &selfNotificationTracer{}
			observers := loggerruntime.WithObservers(tracer, nil)

			detections := handler.ReentryDetections()
			switch kind {
			case "sync":
				logger, err := bootstrap.New(ctx, cfg, observers)
				if err != nil {
					t.Fatal(err)
				}
				for range events {
					if err := logger.Info(ctx, message); err != nil {
						t.Fatal(err)
					}
				}
				if err := logger.Close(); err != nil {
					t.Fatal(err)
				}
			case "async":
				asyncCfg := loggerruntime.DefaultAsyncConfig()
				asyncCfg.LogOutputDestinations, asyncCfg.Stdout, asyncCfg.File = cfg.LogOutputDestinations, nil, cfg.File
				asyncCfg.MaxLogEntryBytes, asyncCfg.MaxParallel = cfg.MaxLogEntryBytes, 1
				logger, err := bootstrap.NewAsync(ctx, asyncCfg, observers)
				if err != nil {
					t.Fatal(err)
				}
				for range events {
					logger.Info(ctx, message)
				}
				if err := logger.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if got := handler.ReentryDetections() - detections; got != 0 {
				t.Errorf("ReentryGuard detected %d re-entrant writes; the wiring must not re-enter the notifying logger", got)
			}
			if tracer.notifications.Load() == 0 {
				t.Errorf("no internal log notification span was started; the nesting check did not observe the notifications")
			}
			if tracer.exceeded.Load() {
				t.Errorf("internal log notifications nested deeper than %d: the file notification target kept writing through the notifying path", selfNotificationNestingLimit)
			}

			paths, err := filepath.Glob(filepath.Join(root, "self-notification", "logs", "*.log"))
			if err != nil {
				t.Fatal(err)
			}
			var lines []string
			for _, path := range paths {
				body, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				for _, line := range strings.Split(string(body), "\n") {
					if strings.TrimSpace(line) != "" {
						lines = append(lines, line)
					}
				}
			}
			var applications, delays int
			for _, line := range lines {
				switch {
				case strings.Contains(line, "[INTERNAL]") && strings.Contains(line, "delayed error"):
					delays++
				case strings.Contains(line, message):
					applications++
				}
			}
			if applications != events || delays != events || len(lines) != 2*events {
				t.Errorf("file lines = %q, want %d application lines and one delay error per write", lines, events)
			}
		})
	}
}
