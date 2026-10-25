package file

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/application/requestctx"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler/reentrytest"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

// reportEveryWriteAsDelayedForTestは、構築済みloggerの書き込み遅延の閾値を負の値にします。
// 経過時間は0以上なので、実時間に依存せず全ての書き込みが遅延errorを一件通知します。
// 構築時の検証（1ms以上）は設定経路にだけ適用されるため、遅い書き込みを決定的に再現できます。
func reportEveryWriteAsDelayedForTest(t *testing.T, logger *FileLogger) {
	t.Helper()
	fsstoreConfig, ok := logger.config.Fsstore().(*appendlog.ConfigAdapter)
	require.True(t, ok, "test logger must use the appendlog config adapter")
	fsstoreConfig.DurationConfig = &appendlog.DurationConfigAdapter{
		WriteFileWarnThresholdDurationConfig:  -time.Nanosecond,
		WriteFileErrorThresholdDurationConfig: -time.Nanosecond,
	}
}

// reportEveryWriteAsNotDelayedForTestは、書き込み遅延の閾値を十分長くし、書き込み時間の記録を
// 有効にします。全ての書き込みが、遅延なしのDEBUGの内部ログを一件ずつ通知します。
func reportEveryWriteAsNotDelayedForTest(t *testing.T, logger *FileLogger) {
	t.Helper()
	fsstoreConfig, ok := logger.config.Fsstore().(*appendlog.ConfigAdapter)
	require.True(t, ok, "test logger must use the appendlog config adapter")
	fsstoreConfig.DurationConfig = &appendlog.DurationConfigAdapter{
		WriteFileWarnThresholdDurationConfig:  time.Hour,
		WriteFileErrorThresholdDurationConfig: time.Hour,
		WriteFileLatencyEnabledConfig:         true,
	}
}

// failActiveFileWritesForTestは、loggerを開いたままactive fileのhandleだけを閉じたものへ
// 置き換え、全ての書き込みattemptをos.ErrClosedで失敗させます。cleanupで元のhandleへ戻し、
// loggerのCloseは通常どおり行えます。logger作成後に呼び出してください。
func failActiveFileWritesForTest(t *testing.T, store *appendlog.FileStore) {
	t.Helper()
	closed, err := os.CreateTemp(t.TempDir(), "closed-active-*.log")
	require.NoError(t, err)
	require.NoError(t, closed.Close())
	store.Mu.Lock()
	active := store.File
	store.File = closed
	store.Mu.Unlock()
	t.Cleanup(func() {
		// 止まった書き込みがwriter lockを持ち続けている場合も、cleanupを止めずに失敗として
		// 報告し、後続のcleanupでloggerを中止して片付けます。
		deadline := time.Now().Add(hangTimeoutForTest)
		for !store.Mu.TryLock() {
			if time.Now().After(deadline) {
				t.Errorf("could not restore the active file handle: the writer lock was still held after %s", hangTimeoutForTest)
				return
			}
			time.Sleep(time.Millisecond)
		}
		store.File = active
		store.Mu.Unlock()
	})
}

// internalLogReentryTestConfigは、background処理を持たないdisable modeのdefault role用設定です。
// 通知の数を数える試験が実時間の書き込み時間（Windowsの時計の分解能など）で遅延の通知を
// 受け取らないよう、書き込み遅延の閾値を十分長くします。遅延の通知を使う試験は、logger作成後に
// reportEveryWriteAsDelayedForTestなどで閾値を置き換えます。
func internalLogReentryTestConfig(t *testing.T) pkglogger.FileLoggerConfig {
	t.Helper()
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "internal-log-reentry")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
	cfg.FsstoreConfig.Duration.WriteFileWarnThresholdDuration = time.Hour
	cfg.FsstoreConfig.Duration.WriteFileErrorThresholdDuration = time.Hour
	return cfg
}

// newInternalLogReentryTestLoggerは、試験の終了時に上限付きで閉じるloggerを作ります。
// 書き込みの停止を検出して失敗した試験は、止まった書き込みを待たずに中止して片付けます。
func newInternalLogReentryTestLogger(t *testing.T, cfg pkglogger.FileLoggerConfig) *FileLogger {
	t.Helper()
	logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
	return logger
}

// failOnUnexpectedInternalLogHandlersは、全てのlevelの通知を失敗として報告するhandlerにします。
// 試験は使うlevelのhandlerだけを、この後で置き換えます。
func failOnUnexpectedInternalLogHandlers(t *testing.T, logger *FileLogger) {
	t.Helper()
	unexpected := func(_ context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
		t.Errorf("unexpected %v internal log notification: %s", entry.Level, entry.Message)
	}
	logger.OnWarn, logger.OnInfo, logger.OnDebug = unexpected, unexpected, unexpected
	logger.OnError = func(ctx context.Context, _ error, entry *logging.LogEntry, tracer observer.Tracer, meter observer.Meter) {
		unexpected(ctx, entry, tracer, meter)
	}
}

func readLogLinesForTest(t *testing.T, path string) []string {
	t.Helper()
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	return splitLogLinesForTest(string(content))
}

func splitLogLinesForTest(text string) []string {
	text = strings.TrimSuffix(text, "\n")
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

// internalLogLinesForTestは、PlainTextFormatterが内部ログの先頭に付ける[INTERNAL]の行を返します。
func internalLogLinesForTest(lines []string) []string {
	var internal []string
	for _, line := range lines {
		if strings.HasPrefix(line, "[INTERNAL]") {
			internal = append(internal, line)
		}
	}
	return internal
}

func applicationLogLinesForTest(lines []string) []string {
	var application []string
	for _, line := range lines {
		if !strings.HasPrefix(line, "[INTERNAL]") {
			application = append(application, line)
		}
	}
	return application
}

func nestedWriteEntry(depth int32) *logging.LogEntry {
	return &logging.LogEntry{
		Level:     pkglogger.LogLevelInfo,
		Message:   "write from the internal log handler",
		RequestID: fmt.Sprintf("nested-%d", depth),
	}
}

// nestedResultsは、handlerからの書き込みの結果を、handlerのgoroutineにかかわらず記録します。
type nestedResults struct {
	mu         sync.Mutex
	requestIDs []string
	errs       []error
}

func (r *nestedResults) add(requestID string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requestIDs = append(r.requestIDs, requestID)
	r.errs = append(r.errs, err)
}

func (r *nestedResults) snapshot() ([]string, []error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.requestIDs...), append([]error(nil), r.errs...)
}

// recordedTextsForTestは、過負荷の処置（dropまたは退避）へ渡された本文を記録します。
type recordedTextsForTest struct {
	mu    sync.Mutex
	texts []string
}

func (r *recordedTextsForTest) record(text string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.texts = append(r.texts, text)
}

func (r *recordedTextsForTest) lines() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return splitLogLinesForTest(strings.Join(r.texts, ""))
}

// requireApplicationLinesForTestは、元の書き込みに続いて、handlerからの書き込みが入れ子の順に
// 記録されたことを確認します。
func requireApplicationLinesForTest(t *testing.T, lines []string, nestedRequestIDs []string) {
	t.Helper()
	application := applicationLogLinesForTest(lines)
	require.Len(t, application, 1+len(nestedRequestIDs), "the original write and every nested write must be recorded")
	require.Contains(t, application[0], "request_id: original")
	for i, requestID := range nestedRequestIDs {
		require.Contains(t, application[1+i], "request_id: "+requestID)
	}
}

// TestFileLoggerReentrantWritesStopForEverySynchronousInternalLogは、書き込みが同期的に
// 発生させる内部ログ（遅延、再試行と失敗、過負荷のdrop・stdout退避）のどれについても、
// handlerからの再入が内部ログごとに一回の通知で止まることを確認します（reentrytest.Modesの
// 全ての書き込み方法）。再入した書き込みの内部ログは通知せず、読み戻せる経路では同じ出力先へ
// 直接記録されたことを確かめます。この直接の記録にもloggerのlog levelを適用し、設定より低い
// 内部ログは記録しません。
// 元の書き込みはhang検出の上限付きで実行します。通知をwriter lockの保持中に行う退行などで
// 書き込みが止まった場合も、試験を止めずに失敗として報告します。再試行の待機はないため
// （MaxWriteAttempts=1）、synctestのbubbleは使いません。bubbleの中ではsync.Mutexの待機で
// 止まった書き込みを上限で検出できないためです。
func TestFileLoggerReentrantWritesStopForEverySynchronousInternalLog(t *testing.T) {
	overloadConfig := func(policy pkgfsstore.OverloadPolicy) func(*pkglogger.FileLoggerConfig) {
		return func(cfg *pkglogger.FileLoggerConfig) {
			cfg.FsstoreConfig.Overload = pkgfsstore.FileArchiveOverloadPolicyConfig{
				OverloadPolicy: policy, SpillDestination: pkgfsstore.SpillDestinationStdout, MinAvailableDiskSpace: 100,
			}
		}
	}
	// requireDirectErrorLinesは、元の書き込みと全ての入れ子の書き込みが記録され、handlerからの
	// 書き込みのERRORの内部ログだけが、通知されずに書き込みごとに一件ずつ同じ出力先へ直接
	// 渡されたことを確認します。元の書き込みの内部ログはhandlerへ通知するため、直接は記録しません。
	requireDirectErrorLines := func(t *testing.T, lines []string, nestedRequestIDs []string) {
		t.Helper()
		requireApplicationLinesForTest(t, lines, nestedRequestIDs)
		internal := internalLogLinesForTest(lines)
		require.Len(t, internal, len(nestedRequestIDs))
		for i, requestID := range nestedRequestIDs {
			require.True(t, strings.HasPrefix(internal[i], "[INTERNAL][ERROR]"), internal[i])
			require.Contains(t, internal[i], requestID)
			require.NotContains(t, internal[i], "original", "the original write's internal log must be notified, not written directly")
		}
	}
	for _, path := range []struct {
		name string
		// eventsPerWriteは、条件が続く間に一回の書き込みが同期的に発生させる内部ログの件数です。
		eventsPerWrite int
		// debugHandlerは、再入するhandlerをOnErrorではなくOnDebugにします。
		debugHandler bool
		configure    func(*pkglogger.FileLoggerConfig)
		inject       func(*testing.T, *FileLogger, *recordedTextsForTest)
		// requireNestedResultは、handlerからの書き込み自身の戻り値を確認します。
		requireNestedResult func(*testing.T, error)
		// requireDirectRecordは、直接記録した内部ログを読み戻せる経路で、その内容を確認します。
		requireDirectRecord func(t *testing.T, logger *FileLogger, spilled *recordedTextsForTest, nestedRequestIDs []string)
	}{
		{
			name:           "write delay",
			eventsPerWrite: 1,
			inject: func(t *testing.T, logger *FileLogger, _ *recordedTextsForTest) {
				reportEveryWriteAsDelayedForTest(t, logger)
			},
			requireNestedResult: func(t *testing.T, err error) { require.NoError(t, err) },
			requireDirectRecord: func(t *testing.T, logger *FileLogger, _ *recordedTextsForTest, nestedRequestIDs []string) {
				requireDirectErrorLines(t, readLogLinesForTest(t, logger.Fsstore.ActiveFilePath), nestedRequestIDs)
			},
		},
		{
			// 設定したlog level（INFO）より低い内部ログ（遅延なしのDEBUG）は、再入した書き込みでも
			// 同じfileへ直接記録しません。元の書き込みのDEBUGは通知先へ渡します。
			name:           "write latency below log level",
			eventsPerWrite: 1,
			debugHandler:   true,
			configure: func(cfg *pkglogger.FileLoggerConfig) {
				cfg.RuntimeConfig.LogLevel = pkglogger.LogLevelInfo
			},
			inject: func(t *testing.T, logger *FileLogger, _ *recordedTextsForTest) {
				reportEveryWriteAsNotDelayedForTest(t, logger)
			},
			requireNestedResult: func(t *testing.T, err error) { require.NoError(t, err) },
			requireDirectRecord: func(t *testing.T, logger *FileLogger, _ *recordedTextsForTest, nestedRequestIDs []string) {
				lines := readLogLinesForTest(t, logger.Fsstore.ActiveFilePath)
				requireApplicationLinesForTest(t, lines, nestedRequestIDs)
				require.Empty(t, internalLogLinesForTest(lines),
					"a DEBUG internal log below LogLevel=INFO must not be written directly")
			},
		},
		{
			// MaxWriteAttempts=1の失敗は、attemptの再試行logと書き込みの失敗を一件ずつ通知します。
			// 直接記録も同じfileへの書き込みで失敗するため、読み戻しは確認できません。
			name:           "write retry and failure",
			eventsPerWrite: 2,
			inject: func(t *testing.T, logger *FileLogger, _ *recordedTextsForTest) {
				reentrytest.DiscardStandardLog(t)
				failActiveFileWritesForTest(t, &logger.Fsstore)
			},
			requireNestedResult: func(t *testing.T, err error) { require.ErrorIs(t, err, os.ErrClosed) },
		},
		{
			name:           "overload drop",
			eventsPerWrite: 1,
			configure:      overloadConfig(pkgfsstore.OverloadPolicyDrop),
			inject: func(_ *testing.T, logger *FileLogger, spilled *recordedTextsForTest) {
				logger.overload.freeAndTotal = func(string) (uint64, uint64, error) { return 99, 1000, nil }
				drop := logger.overload.handle
				logger.overload.handle = func(ctx context.Context, text string) error {
					spilled.record(text)
					return drop(ctx, text)
				}
			},
			requireNestedResult: func(t *testing.T, err error) { require.ErrorIs(t, err, pkglogger.ErrLogDropped) },
			requireDirectRecord: func(t *testing.T, _ *FileLogger, spilled *recordedTextsForTest, nestedRequestIDs []string) {
				requireDirectErrorLines(t, spilled.lines(), nestedRequestIDs)
			},
		},
		{
			name:           "overload degrade to stdout",
			eventsPerWrite: 1,
			configure:      overloadConfig(pkgfsstore.OverloadPolicyDegrade),
			inject: func(_ *testing.T, logger *FileLogger, spilled *recordedTextsForTest) {
				logger.overload.freeAndTotal = func(string) (uint64, uint64, error) { return 99, 1000, nil }
				// stdoutへ退避する代わりに、退避した本文を記録します。
				logger.overload.handle = func(_ context.Context, text string) error {
					spilled.record(text)
					return nil
				}
			},
			requireNestedResult: func(t *testing.T, err error) { require.NoError(t, err) },
			requireDirectRecord: func(t *testing.T, _ *FileLogger, spilled *recordedTextsForTest, nestedRequestIDs []string) {
				requireDirectErrorLines(t, spilled.lines(), nestedRequestIDs)
			},
		},
	} {
		for _, mode := range reentrytest.Modes {
			t.Run(path.name+"/"+mode.Name, func(t *testing.T) {
				cfg := internalLogReentryTestConfig(t)
				if path.configure != nil {
					path.configure(&cfg)
				}
				logger := newInternalLogReentryTestLogger(t, cfg)
				spilled := &recordedTextsForTest{}
				path.inject(t, logger, spilled)
				failOnUnexpectedInternalLogHandlers(t, logger)
				probe := reentrytest.NewProbe(t)
				results := &nestedResults{}
				reenter := func(ctx context.Context) {
					depth, ok := probe.Enter()
					defer probe.Exit()
					if !ok {
						return
					}
					entry := nestedWriteEntry(depth)
					mode.Write(t, ctx, func(ctx context.Context) {
						results.add(entry.RequestID, logger.HandleLogEntry(ctx, entry))
					})
				}
				if path.debugHandler {
					logger.OnDebug = func(ctx context.Context, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
						reenter(ctx)
					}
				} else {
					logger.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
						reenter(ctx)
					}
				}

				// 元の書き込みの戻り値も、handlerからの書き込みと同じ条件の結果です。
				err, finished := reentrytest.CompleteWithin(t, "the original write", func() error {
					return logger.HandleLogEntry(context.Background(), &logging.LogEntry{
						Level: pkglogger.LogLevelInfo, Message: "original write", RequestID: "original",
					})
				})
				if !finished {
					return
				}
				path.requireNestedResult(t, err)

				require.EqualValues(t, reentrytest.HandlerNestingDepth, probe.MaxDepth(), "nesting depth of the handler")
				require.Equal(t, int32(path.eventsPerWrite), probe.Calls(),
					"handler calls for the original write")
				nestedRequestIDs, errs := results.snapshot()
				require.Len(t, errs, path.eventsPerWrite, "every handler call must write once")
				for _, err := range errs {
					path.requireNestedResult(t, err)
				}
				if path.requireDirectRecord != nil {
					path.requireDirectRecord(t, logger, spilled, nestedRequestIDs)
				}
			})
		}
	}
}

// TestFileLoggerIndependentConcurrentDispatchesAreAllNotifiedは、互いに独立したgoroutineの
// 書き込みが多数同時に通知中でも、全ての内部ログがhandlerへ一件ずつ通知され、通知せずに直接
// 記録される内部ログがないことを確認します。再入は通知の数では判定しません。
// 各通知は到着を知らせてから解除まで待つため、全ての到着を受け取った時点で全ての通知が同時に
// 実行中です。到着しない場合（通知をwriter lockの保持中に行い、他の書き込みが止まる退行など）は
// hang検出の上限で失敗します。
func TestFileLoggerIndependentConcurrentDispatchesAreAllNotified(t *testing.T) {
	logger := newInternalLogReentryTestLogger(t, internalLogReentryTestConfig(t))
	reportEveryWriteAsDelayedForTest(t, logger)
	failOnUnexpectedInternalLogHandlers(t, logger)
	release := make(chan struct{})
	// cleanupは後に登録したものから実行するため、loggerのCloseより先に通知を解除します。
	releaseHandlers := releaseOnCleanupForTest(t, release)
	const writers = 32
	arrived := make(chan string, writers)
	var arrivals atomic.Int32
	logger.OnError = func(_ context.Context, _ error, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
		if arrivals.Add(1) > writers {
			// 想定より多い通知は待機せずに失敗させます。
			t.Errorf("unexpected extra internal log notification: %s", entry.Message)
			return
		}
		// 遅延の内部ログは、書き込んだcontextのrequest idを持ちます。
		arrived <- entry.RequestID
		<-release
	}

	results := make(chan error, writers)
	for i := range writers {
		requestID := fmt.Sprintf("writer-%d", i)
		ctx := requestctx.WithRequestID(context.Background(), requestID)
		go func() {
			results <- logger.HandleLogEntry(ctx, &logging.LogEntry{
				Level: pkglogger.LogLevelInfo, Message: "independent write", RequestID: requestID,
			})
		}()
	}
	notified := make(map[string]int, writers)
	for range writers {
		notified[receiveForTest(t, arrived, "internal log notification of every independent write")]++
	}
	// 全ての通知が到着して解除を待っています。この時点で全ての通知が同時に実行中です。
	require.Len(t, notified, writers, "every independent write must reach the handler while %d dispatches are in flight", writers)
	for i := range writers {
		require.Equal(t, 1, notified[fmt.Sprintf("writer-%d", i)], "writer-%d", i)
	}
	require.Empty(t, internalLogLinesForTest(readLogLinesForTest(t, logger.Fsstore.ActiveFilePath)),
		"no internal log of an independent write may be written directly")

	releaseHandlers()
	for range writers {
		require.NoError(t, receiveForTest(t, results, "independent write result"))
	}
	require.EqualValues(t, writers, arrivals.Load())
	require.Len(t, applicationLogLinesForTest(readLogLinesForTest(t, logger.Fsstore.ActiveFilePath)), writers)
}

// TestDeliveryFileLoggerHandlerWriteFailureIsNotRedispatchedは、DeliveryFileLoggerの
// OnErrorから同じloggerへの書き込みが失敗し続けても、通知が止まることを確認します。
// 元の書き込みはhang検出の上限付きで実行し、止まった場合は失敗として報告します。
func TestDeliveryFileLoggerHandlerWriteFailureIsNotRedispatched(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-internal-log-reentry")
			logger, err := NewDeliveryFileLogger(context.Background(), NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
			require.NoError(t, err)
			closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
			reentrytest.DiscardStandardLog(t)
			failActiveFileWritesForTest(t, &logger.Fsstore)

			probe := reentrytest.NewProbe(t)
			var mu sync.Mutex
			var nestedStatuses []pkglogger.DeliveryStatus
			var nestedErrs []error
			logger.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
				depth, ok := probe.Enter()
				defer probe.Exit()
				if !ok {
					return
				}
				mode.Write(t, ctx, func(ctx context.Context) {
					report, err := logger.HandleLogEntry(ctx, nestedWriteEntry(depth))
					mu.Lock()
					defer mu.Unlock()
					nestedErrs = append(nestedErrs, err)
					if len(report.Targets) == 1 {
						nestedStatuses = append(nestedStatuses, report.Targets[0].Status)
					}
				})
			}

			err, finished := reentrytest.CompleteWithin(t, "the original delivery write", func() error {
				_, err := logger.HandleLogEntry(context.Background(), &logging.LogEntry{
					Level: pkglogger.LogLevelInfo, Message: "original write", RequestID: "original",
				})
				return err
			})
			if !finished {
				return
			}
			require.ErrorIs(t, err, os.ErrClosed)
			require.Equal(t, int32(1), probe.Calls())
			require.Equal(t, reentrytest.HandlerNestingDepth, probe.MaxDepth())
			mu.Lock()
			defer mu.Unlock()
			require.Len(t, nestedErrs, 1)
			require.Len(t, nestedStatuses, len(nestedErrs))
			for i, nestedErr := range nestedErrs {
				require.ErrorIs(t, nestedErr, os.ErrClosed)
				require.Equal(t, pkglogger.DeliveryFailed, nestedStatuses[i])
			}
		})
	}
}

// TestDeliveryFileLoggerReentrantWriteErrorIsWrittenToTheSameFileは、OnErrorの中で発生した
// 書き込みerrorが、OnErrorを再び呼ばず同じfileへ直接記録されることを確認します。
// 失敗が続く場合は同じfileにも書けないため、書き込み可能なfileへ再入時の報告を直接行います。
func TestDeliveryFileLoggerReentrantWriteErrorIsWrittenToTheSameFile(t *testing.T) {
	cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-internal-log-direct")
	logger, err := NewDeliveryFileLogger(context.Background(), NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
	var calls atomic.Int32
	logger.OnError = func(context.Context, error, *logging.LogEntry, observer.Tracer, observer.Meter) {
		calls.Add(1)
	}
	failure := errors.New("injected delivery write failure")
	logger.internalLogReentry.Dispatch(context.Background(), func(ctx context.Context) {
		logger.dispatchWriteError(ctx, failure, nestedWriteEntry(1))
	})

	require.Zero(t, calls.Load(), "a write error raised inside OnError must not call OnError again")
	internal := internalLogLinesForTest(readLogLinesForTest(t, logger.Fsstore.ActiveFilePath))
	require.Len(t, internal, 1)
	require.True(t, strings.HasPrefix(internal[0], "[INTERNAL][ERROR]"), internal[0])
	require.Contains(t, internal[0], "request_id: nested-1")
	// 記録したfileの内容（利用者が読む出力）として、内部ログにerrorが残ることを確認します。
	require.Contains(t, internal[0], failure.Error())

	// 通知の外で発生したerrorは、従来どおりOnErrorへ通知します。
	logger.dispatchWriteError(context.Background(), failure, nestedWriteEntry(2))
	require.EqualValues(t, 1, calls.Load())
	require.Len(t, internalLogLinesForTest(readLogLinesForTest(t, logger.Fsstore.ActiveFilePath)), 1)
}

// TestFileLoggerBackgroundNotificationsDispatchFromMarkedContextは、rotation後の処理や
// cleanupなどbackground処理の通知が、再入した書き込みから受け継いだcontextの印や、通知を
// 実行中のgoroutineかどうかにかかわらず、従来どおりhandlerへ通知されることを確認します。
// 同期的な書き込みの内部ログだけが直接記録の対象です。
func TestFileLoggerBackgroundNotificationsDispatchFromMarkedContext(t *testing.T) {
	logger := newInternalLogReentryTestLogger(t, internalLogReentryTestConfig(t))
	var errorCalls, warnCalls atomic.Int32
	var reentered atomic.Bool
	logger.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
		errorCalls.Add(1)
		reentered.Store(logger.internalLogReentry.Reentered(ctx))
	}
	logger.OnWarn = func(context.Context, *logging.LogEntry, observer.Tracer, observer.Meter) {
		warnCalls.Add(1)
	}
	failure := errors.New("background task failure")
	// 入れ子の通知の中で、contextにこのloggerの印がある状態を作ります。
	const nestedDispatches = 3
	var dispatchNested func(context.Context, int)
	dispatchNested = func(ctx context.Context, depth int) {
		if depth == nestedDispatches {
			require.True(t, logger.internalLogReentry.Reentered(ctx))
			logger.wrapAndDispatchError(ctx, failure, logger.InternalCallerErrorEntry("background failure", failure))
			logger.dispatchWarn(ctx, logger.InternalWarnEntry("background warning"))
			return
		}
		logger.internalLogReentry.Dispatch(ctx, func(ctx context.Context) { dispatchNested(ctx, depth+1) })
	}
	dispatchNested(context.Background(), 0)

	require.EqualValues(t, 1, errorCalls.Load())
	require.EqualValues(t, 1, warnCalls.Load())
	require.True(t, reentered.Load(), "the handler must receive a context marked for this logger")
	require.Empty(t, readLogLinesForTest(t, logger.Fsstore.ActiveFilePath), "background notifications must not be written directly")
}
