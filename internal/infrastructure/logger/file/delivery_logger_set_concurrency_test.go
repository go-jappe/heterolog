package file

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// DeliveryLoggerSetの初回生成・handler設定変更・Closeを直列化するlockはchannelで待つため、
// 待機中の呼び出しはsynctestのbubble内で停止状態として扱われます。待機の確認にはsynctest.Waitを
// 使います。sync.Mutexへ戻すとsynctest.Waitが待機を検出できず止まるため、lockの型をここで固定し、
// 変更はcompile errorとして検出します。生成中にmuを保持するなど、bubble内のgoroutineが
// sync.Mutexで止まる退行は、bubbleの実時間の上限（synctestWithHangWatchdogForTest）で失敗します。
// 公開済みroleが生成を待たないことは、その退行も実時間の上限で検出できるようbubbleの外で確認します。
var _ = func(set *DeliveryLoggerSet) *common.WaitableMutex { return &set.creationCloseMu }

type deliveryLoggerSetCreationResult struct {
	logger *DeliveryFileLogger
	err    error
}

func TestDeliveryLoggerSetCachedRolesRemainWritableDuringColdCreation(t *testing.T) {
	ctx := context.Background()
	cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-concurrent-roles")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
	set, err := NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)

	entered := make(chan struct{})
	release := make(chan struct{})
	// cleanupは後に登録したものから実行するため、Closeより先に生成の待機を解除します。
	releaseCreation := releaseOnCleanupForTest(t, release)
	var debugBuilds atomic.Int32
	require.NoError(t, set.ConfigureInternalHandlers(func(role InternalHandlerRole) InternalHandlers {
		if role == InternalHandlerRole(DebugLogger) {
			if debugBuilds.Add(1) == 1 {
				close(entered)
			}
			<-release
		}
		return InternalHandlers{}
	}))
	defaultLogger, err := set.NewFileLogger(ctx)
	require.NoError(t, err)
	errorLogger, err := set.NewErrorFileLogger(ctx)
	require.NoError(t, err)

	const callers = 8
	created := make(chan deliveryLoggerSetCreationResult, callers)
	createDebug := func() {
		logger, err := set.NewDebugFileLogger(ctx)
		created <- deliveryLoggerSetCreationResult{logger: logger, err: err}
	}
	go createDebug()
	receiveForTest(t, entered, "debug role handler factory entry")
	for range callers - 1 {
		go createDebug()
	}

	// 未生成roleを止めたまま、公開済みroleのlookupと実file書込が進むことを確認します。
	written := make(chan deliveryRoleWriteResult, 1)
	go func() {
		report, err := set.LogicalLogger().HandleLogEntry(ctx, &logging.LogEntry{
			Level: pkglogger.LogLevelInfo, Message: "logical default during debug creation",
		})
		written <- deliveryRoleWriteResult{report: report, err: err}
	}()
	fallbackDone := make(chan struct{})
	go func() {
		set.NewNamedErrorFallbackHandler().Handler(ctx, nil, &logging.LogEntry{
			Level: pkglogger.LogLevelError, Message: "error fallback during debug creation",
		}, observer.NopTracer{}, observer.NopMeter{})
		close(fallbackDone)
	}()
	result := receiveForTest(t, written, "cached logical role write while an unrelated role is constructed")
	requireDeliverySucceeded(t, result.report, result.err)
	receiveForTest(t, fallbackDone, "cached error fallback while an unrelated role is constructed")
	require.NotEqual(t, defaultLogger.Fsstore.ActiveFilePath, errorLogger.Fsstore.ActiveFilePath)
	assertLogFileContains(t, defaultLogger.Fsstore.ActiveFilePath, "logical default during debug creation")
	assertLogFileNotContains(t, defaultLogger.Fsstore.ActiveFilePath, "error fallback during debug creation")
	assertLogFileContains(t, errorLogger.Fsstore.ActiveFilePath, "error fallback during debug creation")
	assertLogFileNotContains(t, errorLogger.Fsstore.ActiveFilePath, "logical default during debug creation")

	releaseCreation()
	var debugLogger *DeliveryFileLogger
	for range callers {
		result := receiveForTest(t, created, "debug role creation after releasing the factory")
		require.NoError(t, result.err)
		if debugLogger == nil {
			debugLogger = result.logger
		}
		require.Same(t, debugLogger, result.logger)
	}
	require.EqualValues(t, 1, debugBuilds.Load(), "concurrent requests must share one physical role")
}

type deliveryRoleWriteResult struct {
	report pkglogger.DeliveryReport
	err    error
}

func TestDeliveryLoggerSetCloseWaitsForUnpublishedRole(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		ctx := context.Background()
		cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-close-during-creation")
		set, err := NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)

		entered := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		// releasedは生成の待機を解除した後だけtrueです。Closeが戻った時点で読み、解除前に戻ったかを判定します。
		var released atomic.Bool
		releaseCreation := func() { releaseOnce.Do(func() { released.Store(true); close(release) }) }
		// 生成の待機を解除してから、上限付きで閉じます。
		t.Cleanup(func() {
			releaseCreation()
			closeWithinBoundForTest(t, boundedwait.BubbleTimeout, "set Close", set, true)
		})
		require.NoError(t, set.ConfigureInternalHandlers(func(InternalHandlerRole) InternalHandlers {
			close(entered)
			<-release
			return InternalHandlers{}
		}))
		created := make(chan deliveryLoggerSetCreationResult, 1)
		go func() {
			logger, err := set.NewFileLogger(ctx)
			created <- deliveryLoggerSetCreationResult{logger: logger, err: err}
		}()
		receiveForTest(t, entered, "default role handler factory entry")

		type closeResult struct {
			err          error
			afterRelease bool
		}
		closed := make(chan closeResult, 1)
		go func() {
			err := set.Close()
			closed <- closeResult{err: err, afterRelease: released.Load()}
		}()
		// Closeが受付を停止し、未公開roleの生成が保持するlockを待つまで進めてから、完了待ちを確認します。
		synctest.Wait()
		set.mu.Lock()
		closeState := set.closeState
		set.mu.Unlock()
		require.Equal(t, common.LoggerCloseStateStarted, closeState)
		// mapへ未公開のresourceもClose対象になるまで、所有者のCloseは完了しません。
		require.Empty(t, closed, "Close overtook an unpublished role")
		releaseCreation()
		result := receiveForTest(t, created, "role creation after releasing the factory")
		require.NoError(t, result.err)
		require.NotNil(t, result.logger)
		closedResult := receiveForTest(t, closed, "DeliveryLoggerSet Close after role publication")
		require.NoError(t, closedResult.err)
		require.True(t, closedResult.afterRelease, "Close overtook an unpublished role")
		require.Nil(t, result.logger.Fsstore.File, "Close must include the role created before it")
		_, err = set.NewFileLogger(ctx)
		require.ErrorIs(t, err, ErrLoggerSetClosed)

		// 全resourceを閉じた後にdirectory leaseも解放されています。
		replacement, err := NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		require.NoError(t, closeBoundedForTest(t, boundedwait.BubbleTimeout, "replacement Close", replacement))
	})
}

func TestDeliveryLoggerSetConfigureWaitsForUnpublishedRole(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		ctx := context.Background()
		cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-configure-during-creation")
		set, err := NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)

		entered := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		// releasedは生成の待機を解除した後だけtrueです。設定変更が戻った時点で読み、解除前に戻ったかを判定します。
		var released atomic.Bool
		releaseCreation := func() { releaseOnce.Do(func() { released.Store(true); close(release) }) }
		// 生成の待機を解除してから、上限付きで閉じます。
		t.Cleanup(func() {
			releaseCreation()
			closeWithinBoundForTest(t, boundedwait.BubbleTimeout, "set Close", set, true)
		})
		var originalBuilds, replacementBuilds atomic.Int32
		require.NoError(t, set.ConfigureInternalHandlers(func(role InternalHandlerRole) InternalHandlers {
			originalBuilds.Add(1)
			if role == InternalHandlerRole(DefaultLogger) {
				close(entered)
				<-release
			}
			return InternalHandlers{}
		}))
		created := make(chan deliveryLoggerSetCreationResult, 1)
		go func() {
			logger, err := set.NewFileLogger(ctx)
			created <- deliveryLoggerSetCreationResult{logger: logger, err: err}
		}()
		receiveForTest(t, entered, "default role handler factory entry")

		type configureResult struct {
			err          error
			afterRelease bool
		}
		configured := make(chan configureResult, 1)
		go func() {
			err := set.ConfigureInternalHandlers(func(InternalHandlerRole) InternalHandlers {
				replacementBuilds.Add(1)
				return InternalHandlers{}
			})
			configured <- configureResult{err: err, afterRelease: released.Load()}
		}()
		// 未公開でmapが空でも、生成中のfactoryを別の設定で追い越してはいけません。
		// 設定変更が生成の保持するlockを待つまで進めてから、未完了であることを確認します。
		synctest.Wait()
		require.Empty(t, configured, "ConfigureInternalHandlers overtook an unpublished role")
		releaseCreation()
		result := receiveForTest(t, created, "role creation after releasing the factory")
		require.NoError(t, result.err)
		require.NotNil(t, result.logger)
		configuredResult := receiveForTest(t, configured, "ConfigureInternalHandlers after role publication")
		require.ErrorIs(t, configuredResult.err, ErrLoggerSetAlreadyStarted)
		require.True(t, configuredResult.afterRelease, "ConfigureInternalHandlers overtook an unpublished role")
		_, err = set.NewErrorFileLogger(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 2, originalBuilds.Load())
		require.Zero(t, replacementBuilds.Load(), "rejected factory must not replace the original")
	})
}

func TestDeliveryLoggerSetFailedCreationRemainsUnpublishedAndCanRetry(t *testing.T) {
	ctx := context.Background()
	cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-retry-creation")
	set, err := NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
	defaultLogger, err := set.NewFileLogger(ctx)
	require.NoError(t, err)

	// error roleのdirectoryだけを通常fileで塞ぎ、実constructorの失敗を発生させます。
	errorDirectory := filepath.Join(
		cfg.FsstoreConfig.Layout.LogPath,
		cfg.FsstoreConfig.Layout.Target,
		cfg.FsstoreConfig.Layout.ErrorLogDir,
	)
	require.NoError(t, os.MkdirAll(filepath.Dir(errorDirectory), 0o700))
	require.NoError(t, os.WriteFile(errorDirectory, []byte("directory creation blocker"), 0o600))
	failedLogger, err := set.NewErrorFileLogger(ctx)
	require.Error(t, err)
	require.Nil(t, failedLogger)
	set.mu.Lock()
	published := set.loggers[ErrorLogger]
	set.mu.Unlock()
	require.Nil(t, published, "failed resource must not be cached")

	require.NoError(t, os.Rename(errorDirectory, errorDirectory+".blocked"))
	retried := make(chan deliveryLoggerSetCreationResult, 1)
	go func() {
		logger, err := set.NewErrorFileLogger(ctx)
		retried <- deliveryLoggerSetCreationResult{logger: logger, err: err}
	}()
	// 失敗した生成が生成のlockを解放していなければ、再試行は戻らずhang検出の上限で失敗します。
	result := receiveForTest(t, retried, "error role construction retry after a failed construction")
	require.NoError(t, result.err)
	require.NotNil(t, result.logger)
	errorLogger := result.logger
	shared, err := set.NewErrorFileLogger(ctx)
	require.NoError(t, err)
	require.Same(t, errorLogger, shared)
	report, err := set.LogicalLogger().HandleLogEntry(ctx, &logging.LogEntry{
		Level: pkglogger.LogLevelError, Message: "delivery after constructor retry",
	})
	requireDeliverySucceeded(t, report, err)
	assertLogFileContains(t, errorLogger.Fsstore.ActiveFilePath, "delivery after constructor retry")
	assertLogFileNotContains(t, defaultLogger.Fsstore.ActiveFilePath, "delivery after constructor retry")
}
