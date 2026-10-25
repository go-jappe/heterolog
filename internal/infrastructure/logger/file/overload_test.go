package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func newOverloadTestLogger(t *testing.T, cfg pkglogger.FileLoggerConfig, role loggerType) *FileLogger {
	t.Helper()
	adapter := NewConfigAdapter(cfg)
	var logger *FileLogger
	var err error
	switch role {
	case ErrorLogger:
		logger, err = newStandaloneErrorFileLogger(context.Background(), adapter, nil, nil)
	case DebugLogger:
		logger, err = newStandaloneDebugFileLogger(context.Background(), adapter, nil, nil)
	default:
		logger, err = newStandaloneFileLogger(context.Background(), adapter, nil, nil)
	}
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
	return logger
}

func fileOverloadTestConfig(t *testing.T, mode pkgfsstore.FileLifecycleMode, policy pkgfsstore.OverloadPolicy) pkglogger.FileLoggerConfig {
	t.Helper()
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "overload")
	cfg.LifecycleMode = mode
	cfg.FsstoreConfig.Overload = pkgfsstore.FileArchiveOverloadPolicyConfig{
		OverloadPolicy: policy, SpillDestination: pkgfsstore.SpillDestinationStdout, MinAvailableDiskSpace: 100,
	}
	// 通知を数える試験が過負荷の通知だけを数えるよう、書き込み遅延の閾値を十分長くします。
	// 共通設定の1msでは、Windowsの時計の分解能（約15.6ms）などで遅延のerror通知が加わります。
	cfg.FsstoreConfig.Duration.WriteFileWarnThresholdDuration = time.Hour
	cfg.FsstoreConfig.Duration.WriteFileErrorThresholdDuration = time.Hour
	return cfg
}

// stubOverloadSpillForTestは、degradeの退避先だけを試験用に置き換えます。
// dropでは実際の破棄処理を使い、破棄の結果を呼び出し元へ返すことを確認します。
func stubOverloadSpillForTest(logger *FileLogger, policy pkgfsstore.OverloadPolicy) {
	if policy == pkgfsstore.OverloadPolicyDegrade {
		logger.overload.handle = func(context.Context, string) error { return nil }
	}
}

// requireOverloadWriteResultは、退避した記録はnil、破棄した記録はErrLogDroppedで返ることを確認します。
func requireOverloadWriteResult(t *testing.T, policy pkgfsstore.OverloadPolicy, err error) {
	t.Helper()
	if policy == pkgfsstore.OverloadPolicyDrop {
		require.ErrorIs(t, err, pkglogger.ErrLogDropped)
		return
	}
	require.NoError(t, err)
}

func TestFileOverloadThresholdAcrossModesAndRoles(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner, pkgfsstore.FileLifecycleModeDisable} {
		for _, role := range []loggerType{DefaultLogger, ErrorLogger, DebugLogger} {
			for _, policy := range []pkgfsstore.OverloadPolicy{pkgfsstore.OverloadPolicyDrop, pkgfsstore.OverloadPolicyDegrade} {
				t.Run(string(mode)+"/"+string(role)+"/"+string(policy), func(t *testing.T) {
					logger := newOverloadTestLogger(t, fileOverloadTestConfig(t, mode, policy), role)
					level := pkglogger.LogLevelInfo
					if role == ErrorLogger {
						level = pkglogger.LogLevelError
					}
					if role == DebugLogger {
						level = pkglogger.LogLevelDebug
					}
					entry := &logging.LogEntry{OccurredAt: logger.Clock.Now(), Level: level, Message: "below threshold"}
					var observedDirectories, spilled []string
					free := uint64(99)
					logger.overload.freeAndTotal = func(directory string) (uint64, uint64, error) {
						observedDirectories = append(observedDirectories, directory)
						return free, 1000, nil
					}
					if policy == pkgfsstore.OverloadPolicyDegrade {
						logger.overload.handle = func(_ context.Context, text string) error { spilled = append(spilled, text); return nil }
					}
					var notifications []error
					logger.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
						notifications = append(notifications, err)
					}
					active := logger.Fsstore.ActiveFilePath
					// 退避せずに破棄した記録は、呼び出し元がErrLogDroppedで判別できます。
					requireOverloadWriteResult(t, policy, logger.HandleLogEntry(context.Background(), entry))
					content, err := os.ReadFile(active)
					require.NoError(t, err)
					require.Empty(t, content)
					require.Len(t, notifications, 1)
					require.ErrorIs(t, notifications[0], ErrFileStorageOverloaded)
					if policy == pkgfsstore.OverloadPolicyDegrade {
						require.Equal(t, []string{logger.LogFormatter.Format(entry) + "\n"}, spilled)
					} else {
						require.Empty(t, spilled)
					}
					for _, available := range []uint64{100, 101} {
						free = available
						require.NoError(t, logger.HandleLogEntry(context.Background(), &logging.LogEntry{Level: level, Message: "available"}))
					}
					require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger))
					require.Equal(t, []string{filepath.Dir(active), filepath.Dir(active), filepath.Dir(active)}, observedDirectories)
					require.Len(t, notifications, 1)
					content, err = os.ReadFile(active)
					require.NoError(t, err)
					require.NotContains(t, string(content), "below threshold")
					require.Contains(t, string(content), "available")
				})
			}
		}
	}
}

func TestFileOverloadDoesNotRotateOrGrowFiles(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner, pkgfsstore.FileLifecycleModeDisable} {
		for _, policy := range []pkgfsstore.OverloadPolicy{pkgfsstore.OverloadPolicyDrop, pkgfsstore.OverloadPolicyDegrade} {
			t.Run(string(mode)+"/"+string(policy), func(t *testing.T) {
				cfg := fileOverloadTestConfig(t, mode, policy)
				cfg.FsstoreConfig.Lifecycle.EnableRotate = true
				logger := newOverloadTestLogger(t, cfg, DefaultLogger)
				active := logger.Fsstore.ActiveFilePath
				require.NoError(t, os.Truncate(active, int64(cfg.FsstoreConfig.Lifecycle.MaxFileSize)))
				logger.overload.freeAndTotal = func(string) (uint64, uint64, error) { return 99, 1000, nil }
				stubOverloadSpillForTest(logger, policy)
				requireOverloadWriteResult(t, policy, logger.HandleLogEntry(context.Background(), &logging.LogEntry{Level: pkglogger.LogLevelInfo, Message: "would rotate"}))
				require.Equal(t, active, logger.Fsstore.ActiveFilePath)
				require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger))
				info, err := os.Stat(active)
				require.NoError(t, err)
				require.EqualValues(t, cfg.FsstoreConfig.Lifecycle.MaxFileSize, info.Size())
				entries, err := os.ReadDir(filepath.Dir(active))
				require.NoError(t, err)
				require.Len(t, entries, 1)
			})
		}
	}
}

func TestFileOverloadZeroThresholdDoesNotProbe(t *testing.T) {
	cfg := fileOverloadTestConfig(t, pkgfsstore.FileLifecycleModeDisable, pkgfsstore.OverloadPolicyDrop)
	cfg.FsstoreConfig.Overload.MinAvailableDiskSpace = 0
	logger := newOverloadTestLogger(t, cfg, DefaultLogger)
	logger.overload.freeAndTotal = func(string) (uint64, uint64, error) {
		t.Error("capacity probe with disabled threshold")
		return 0, 0, errors.New("unexpected probe")
	}
	require.NoError(t, logger.HandleLogEntry(context.Background(), &logging.LogEntry{Level: pkglogger.LogLevelInfo, Message: "disabled check"}))
	content, err := os.ReadFile(logger.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	require.Contains(t, string(content), "disabled check")
}

func TestFileOverloadConstructorRejectsInvalidSettings(t *testing.T) {
	for _, tc := range []struct {
		cfg  pkgfsstore.FileArchiveOverloadPolicyConfig
		want error
	}{
		{cfg: pkgfsstore.FileArchiveOverloadPolicyConfig{OverloadPolicy: "unknown"}, want: pkgfsstore.ErrUnsupportedOverloadPolicy},
		{
			cfg:  pkgfsstore.FileArchiveOverloadPolicyConfig{OverloadPolicy: "unknown", SpillDestination: pkgfsstore.SpillDestinationStdout},
			want: pkgfsstore.ErrUnsupportedOverloadPolicy,
		},
		{
			cfg:  pkgfsstore.FileArchiveOverloadPolicyConfig{OverloadPolicy: pkgfsstore.OverloadPolicyDegrade, SpillDestination: "s3"},
			want: pkgfsstore.ErrUnsupportedSpillDestination,
		},
		{
			cfg:  pkgfsstore.FileArchiveOverloadPolicyConfig{OverloadPolicy: pkgfsstore.OverloadPolicyDegrade, SpillDestination: "nfs"},
			want: pkgfsstore.ErrUnsupportedSpillDestination,
		},
	} {
		t.Run(string(tc.cfg.OverloadPolicy)+"/"+string(tc.cfg.SpillDestination), func(t *testing.T) {
			config := fileOverloadTestConfig(t, pkgfsstore.FileLifecycleModeDisable, tc.cfg.OverloadPolicy)
			config.FsstoreConfig.Overload = tc.cfg
			logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(config), nil, nil)
			require.ErrorIs(t, err, tc.want)
			require.Nil(t, logger)
		})
	}
}

func TestFileOverloadFailuresNotifyAndRecoverOnNextWrite(t *testing.T) {
	for _, stage := range []string{"probe", "spill"} {
		for _, variant := range []struct {
			name       string
			role       loggerType
			level      pkglogger.LogLevel
			stackTrace bool
		}{
			{name: "normal", role: DefaultLogger, level: pkglogger.LogLevelInfo},
			{name: "stack trace", role: ErrorLogger, level: pkglogger.LogLevelError, stackTrace: true},
		} {
			t.Run(stage+"/"+variant.name, func(t *testing.T) {
				logger := newOverloadTestLogger(t, fileOverloadTestConfig(t, pkgfsstore.FileLifecycleModeDisable, pkgfsstore.OverloadPolicyDegrade), variant.role)
				failure := errors.New(stage + " failed")
				failed := true
				logger.overload.freeAndTotal = func(string) (uint64, uint64, error) {
					if failed && stage == "probe" {
						return 0, 0, failure
					}
					if failed {
						return 99, 1000, nil
					}
					return 100, 1000, nil
				}
				logger.overload.handle = func(context.Context, string) error { return failure }
				var notified []error
				logger.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
					notified = append(notified, err)
				}
				err := logger.HandleLogEntry(context.Background(), &logging.LogEntry{
					Level: variant.level, Message: "failed write", StackTraceRequested: variant.stackTrace,
					Err: errors.New("application error"),
				})
				require.ErrorIs(t, err, failure)
				require.Len(t, notified, 1)
				require.ErrorIs(t, notified[0], failure)
				failed = false
				require.NoError(t, logger.HandleLogEntry(context.Background(), &logging.LogEntry{Level: variant.level, Message: "recovered write"}))
				require.Len(t, notified, 1)
				content, err := os.ReadFile(logger.Fsstore.ActiveFilePath)
				require.NoError(t, err)
				require.NotContains(t, string(content), "failed write")
				require.Contains(t, string(content), "recovered write")
			})
		}
	}
}

func TestFileOverloadFallbackDoesNotRedispatchErrors(t *testing.T) {
	for _, stage := range []string{"drop", "probe", "spill"} {
		t.Run(stage, func(t *testing.T) {
			logger := newOverloadTestLogger(t, fileOverloadTestConfig(t, pkgfsstore.FileLifecycleModeDisable, pkgfsstore.OverloadPolicyDrop), DefaultLogger)
			failure := errors.New(stage + " failed")
			logger.overload.freeAndTotal = func(string) (uint64, uint64, error) {
				if stage == "probe" {
					return 0, 0, failure
				}
				return 99, 1000, nil
			}
			if stage == "spill" {
				logger.overload.handle = func(context.Context, string) error { return failure }
			}
			logger.OnError = func(context.Context, error, *logging.LogEntry, observer.Tracer, observer.Meter) {
				t.Error("fallback recursively dispatched OnError")
			}
			err := logger.writeFallbackEntry(context.Background(), &logging.LogEntry{Level: pkglogger.LogLevelInfo, Message: "fallback"})
			if stage == "drop" {
				require.ErrorIs(t, err, pkglogger.ErrLogDropped)
			} else {
				require.ErrorIs(t, err, failure)
			}
		})
	}
}

func TestFileOverloadNotificationCanRequestCloseOrReenterLogger(t *testing.T) {
	for _, action := range []string{"close", "reenter"} {
		t.Run(action, func(t *testing.T) {
			synctestWithHangWatchdogForTest(t, func(t *testing.T) {
				logger := newOverloadTestLogger(t, fileOverloadTestConfig(t, pkgfsstore.FileLifecycleModeDisable, pkgfsstore.OverloadPolicyDrop), DefaultLogger)
				free := uint64(99)
				logger.overload.freeAndTotal = func(string) (uint64, uint64, error) { return free, 1000, nil }
				calls := 0
				var callbackErr error
				logger.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
					calls++
					if action == "close" {
						logger.RequestClose()
						return
					}
					free = 100
					callbackErr = logger.HandleLogEntry(ctx, &logging.LogEntry{Level: pkglogger.LogLevelInfo, Message: "reentrant write"})
				}
				require.ErrorIs(t, logger.HandleLogEntry(context.Background(), &logging.LogEntry{Level: pkglogger.LogLevelInfo, Message: "overloaded"}), pkglogger.ErrLogDropped)
				require.NoError(t, callbackErr)
				require.Equal(t, 1, calls)
				if action == "close" {
					require.NoError(t, closeBoundedForTest(t, boundedwait.BubbleTimeout, "logger Close", logger))
				}
			})
		})
	}
}

func TestFileOverloadCloseWaitsForAcceptedSpill(t *testing.T) {
	synctestWithHangWatchdogForTest(t, func(t *testing.T) {
		logger := newOverloadTestLogger(t, fileOverloadTestConfig(t, pkgfsstore.FileLifecycleModeDisable, pkgfsstore.OverloadPolicyDegrade), DefaultLogger)
		logger.overload.freeAndTotal = func(string) (uint64, uint64, error) { return 99, 1000, nil }
		started, release := make(chan struct{}), make(chan struct{})
		releaseSpill := releaseOnCleanupForTest(t, release)
		logger.overload.handle = func(context.Context, string) error { close(started); <-release; return nil }
		writeDone, closeDone := make(chan error, 1), make(chan error, 1)
		go func() {
			writeDone <- logger.HandleLogEntry(context.Background(), &logging.LogEntry{Level: pkglogger.LogLevelInfo, Message: "spill"})
		}()
		receiveForTest(t, started, "accepted spill start")
		go func() { closeDone <- logger.Close() }()
		synctest.Wait()
		require.Empty(t, closeDone, "Close must wait for accepted spill")
		releaseSpill()
		require.NoError(t, receiveForTest(t, writeDone, "spilled write completion"))
		require.NoError(t, receiveForTest(t, closeDone, "Close after the accepted spill"))
	})
}

func TestFileOverloadStillTriggersDueInlinePurge(t *testing.T) {
	for _, policy := range []pkgfsstore.OverloadPolicy{pkgfsstore.OverloadPolicyDrop, pkgfsstore.OverloadPolicyDegrade} {
		t.Run(string(policy), func(t *testing.T) {
			logger := newOverloadTestLogger(t, fileOverloadTestConfig(t, pkgfsstore.FileLifecycleModeInline, policy), DefaultLogger)
			logger.overload.freeAndTotal = func(string) (uint64, uint64, error) { return 99, 1000, nil }
			stubOverloadSpillForTest(logger, policy)
			pattern := logger.Fsstore.FileName
			old := logger.Clock.Now().AddDate(0, 0, -logger.inlinePurge.config.MaxFileAge()-1)
			expiredPath := filepath.Join(pattern.Dir, pattern.Prefix+pattern.Target+"_"+old.Format(pkgfsstore.FileDateFormat)+"_1.log")
			require.NoError(t, os.WriteFile(expiredPath, []byte("expired"), 0o600))
			scans := observeCleanupScans(logger)
			requireOverloadWriteResult(t, policy, logger.HandleLogEntry(context.Background(), &logging.LogEntry{Level: pkglogger.LogLevelInfo, Message: "not written"}))
			require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger))
			require.Positive(t, scans.reads.Load())
			require.NoFileExists(t, expiredPath)
			content, err := os.ReadFile(logger.Fsstore.ActiveFilePath)
			require.NoError(t, err)
			require.Empty(t, content)
		})
	}
}

func TestFileOverloadDoesNotAddDiscardedBytesToUsageEstimate(t *testing.T) {
	for _, policy := range []pkgfsstore.OverloadPolicy{pkgfsstore.OverloadPolicyDrop, pkgfsstore.OverloadPolicyDegrade} {
		t.Run(string(policy), func(t *testing.T) {
			logger := newOverloadTestLogger(t, fileOverloadTestConfig(t, pkgfsstore.FileLifecycleModeInline, policy), DefaultLogger)
			logger.Clock = clock.NewFixedClock(logger.Clock.Now())
			logger.overload.freeAndTotal = func(string) (uint64, uint64, error) { return 99, 1000, nil }
			stubOverloadSpillForTest(logger, policy)
			scans := observeCleanupScans(logger)
			require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
			baselineScans := scans.reads.Load()
			runtime := logger.inlinePurge
			require.True(t, runtime.directorySizeTracker.TryStartObservation(logger.Clock.Now(), runtime.config, true))
			runtime.directorySizeTracker.FinishObservation(runtime.config.MaxDirSize() - 1)
			// 容量上限の1byte手前でも、退避・破棄した本文は実ディレクトリの増分ではありません。
			requireOverloadWriteResult(t, policy, logger.HandleLogEntry(context.Background(), &logging.LogEntry{Level: pkglogger.LogLevelInfo, Message: "discarded bytes must not trigger a capacity scan"}))
			require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger))
			require.Equal(t, baselineScans, scans.reads.Load())
			require.False(t, runtime.directorySizeTracker.TryStartObservation(logger.Clock.Now(), runtime.config, false))
		})
	}
}
