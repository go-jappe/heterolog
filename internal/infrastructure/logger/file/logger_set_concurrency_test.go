package file

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// LoggerSetの初回生成・Closeを直列化するlockはchannelで待つため、待機中の呼び出しは
// synctestのbubble内で停止状態として扱われます。bubble内の試験はsynctest.Waitで待機に入った
// ことを確かめてから未完了であることを確認し、時間の窓やgoroutineのstackに頼りません。
// sync.Mutexへ戻すとsynctest.Waitが待機を検出できず止まるため、lockの型をここで固定し、
// 変更はcompile errorとして検出します。生成中にmuを保持するなど、bubble内のgoroutineが
// sync.Mutexで止まる退行は、bubbleの実時間の上限（synctestWithHangWatchdogForTest）で失敗します。
// 生成済みroleが生成を待たないことは、その退行も実時間の上限で検出できるようbubbleの外で確認します。
var _ = func(set *LoggerSet) *common.WaitableMutex { return &set.creationCloseMu }

// coldErrorRoleFixtureは、error roleの初回構築をhandler factoryの中で止めたLoggerSetです。
// default・debug roleは生成済みです。releasedは構築の待機を解除した後だけtrueです。
// 完了時に読み、解除前に完了したかを判定します。
type coldErrorRoleFixture struct {
	set                *LoggerSet
	defaultLogger      *FileLogger
	debugLogger        *FileLogger
	entered            chan struct{}
	unblock            func()
	released           atomic.Bool
	errorConstructions atomic.Int32
}

// newColdErrorRoleFixtureForTestは、coldErrorRoleFixtureを作ります。cleanupでは構築の待機を
// 解除してから、timeout（bubble内ならboundedwait.BubbleTimeout、bubble外なら
// boundedwait.HangTimeout）の上限付きでLoggerSetを閉じます。
func newColdErrorRoleFixtureForTest(t *testing.T, timeout time.Duration) *coldErrorRoleFixture {
	t.Helper()
	ctx := context.Background()
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "cold-role-fallback")
	cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
	set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	fixture := &coldErrorRoleFixture{set: set, entered: make(chan struct{})}
	release := make(chan struct{})
	var releaseOnce sync.Once
	fixture.unblock = func() { releaseOnce.Do(func() { fixture.released.Store(true); close(release) }) }
	// 構築の待機を解除してから、上限付きで閉じます。
	t.Cleanup(func() {
		fixture.unblock()
		closeWithinBoundForTest(t, timeout, "set Close", set, true)
	})
	require.NoError(t, set.ConfigureInternalHandlers(func(role InternalHandlerRole) InternalHandlers {
		if role == InternalHandlerRole(ErrorLogger) {
			if fixture.errorConstructions.Add(1) == 1 {
				close(fixture.entered)
			}
			<-release
		}
		return InternalHandlers{}
	}))
	fixture.defaultLogger, err = set.NewFileLogger(ctx)
	require.NoError(t, err)
	fixture.debugLogger, err = set.NewDebugFileLogger(ctx)
	require.NoError(t, err)
	return fixture
}

type coldRoleLookupResult struct {
	logger *FileLogger
	err    error
}

// startColdErrorRoleCallersForTestは、error roleの初回構築を開始して構築の待機に入るまで進め、
// 同じroleのlookupをさらにlookups件と、error roleのfallbackを一件開始します。lookupの結果は
// resultsへ、fallbackの完了時にreleasedだったかはfallbackDoneへ届きます。
func startColdErrorRoleCallersForTest(t *testing.T, fixture *coldErrorRoleFixture, lookups int) (<-chan coldRoleLookupResult, <-chan bool) {
	t.Helper()
	ctx := context.Background()
	results := make(chan coldRoleLookupResult, lookups+1)
	lookup := func() {
		logger, err := fixture.set.NewErrorFileLogger(ctx)
		results <- coldRoleLookupResult{logger: logger, err: err}
	}
	go lookup()
	receiveForTest(t, fixture.entered, "cold error construction start")
	for range lookups {
		go lookup()
	}
	fallbackDone := make(chan bool, 1)
	go func() {
		fixture.set.newErrorFallbackHandler()(ctx, os.ErrInvalid, &logging.LogEntry{
			Level: pkglogger.LogLevelError, Message: "cold error fallback",
		}, nil, nil)
		fallbackDone <- fixture.released.Load()
	}()
	return results, fallbackDone
}

// requireColdErrorRoleSharedForTestは、構築の解除後に、同じroleの全てのlookupが一つの
// instanceを受け取り、構築が一回だけ行われ、fallbackの内容がerror roleのfileへ書き込まれたことを
// 確認し、そのinstanceを返します。
func requireColdErrorRoleSharedForTest(t *testing.T, fixture *coldErrorRoleFixture, results <-chan coldRoleLookupResult, lookups int) *FileLogger {
	t.Helper()
	var errorLogger *FileLogger
	for range lookups + 1 {
		got := receiveForTest(t, results, "same-role lookup after construction")
		require.NoError(t, got.err)
		if errorLogger == nil {
			errorLogger = got.logger
		} else {
			require.Same(t, errorLogger, got.logger)
		}
	}
	require.EqualValues(t, 1, fixture.errorConstructions.Load(), "concurrent requests must share one physical role")
	errorContent, err := os.ReadFile(errorLogger.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	require.Contains(t, string(errorContent), "cold error fallback")
	require.NotEqual(t, fixture.defaultLogger.Fsstore.ActiveFilePath, errorLogger.Fsstore.ActiveFilePath)
	return errorLogger
}

// TestLoggerSetCachedFallbacksProceedDuringColdRoleConstructionは、未生成roleの初回構築を
// 止めたまま、生成済みroleの実fallbackが同じLoggerSetから書き込めることを確認します。
// bubbleの外で実行するため、構築中にmuを保持して生成済みroleの取得まで待たせる退行も、
// hang検出の上限で失敗として報告します。
func TestLoggerSetCachedFallbacksProceedDuringColdRoleConstruction(t *testing.T) {
	ctx := context.Background()
	fixture := newColdErrorRoleFixtureForTest(t, boundedwait.HangTimeout)
	const concurrentLookups = 8
	results, errorFallbackDone := startColdErrorRoleCallersForTest(t, fixture, concurrentLookups)

	// errorの初回構築を止めたまま、生成済みroleの実fallbackを同じSetから取得します。
	warmFallbackDone := make(chan struct{}, 2)
	go func() {
		fixture.set.newWarnFallbackHandler()(ctx, &logging.LogEntry{
			Level: pkglogger.LogLevelWarn, Message: "warm default fallback",
		}, nil, nil)
		warmFallbackDone <- struct{}{}
	}()
	go func() {
		fixture.set.newDebugFallbackHandler()(ctx, &logging.LogEntry{
			Level: pkglogger.LogLevelDebug, Message: "warm debug fallback",
		}, nil, nil)
		warmFallbackDone <- struct{}{}
	}()
	for range 2 {
		receiveForTest(t, warmFallbackDone, "cached fallback while an unrelated cold role is constructed")
	}
	defaultContent, err := os.ReadFile(fixture.defaultLogger.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	debugContent, err := os.ReadFile(fixture.debugLogger.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	require.NotEqual(t, fixture.defaultLogger.Fsstore.ActiveFilePath, fixture.debugLogger.Fsstore.ActiveFilePath)
	require.Contains(t, string(defaultContent), "warm default fallback")
	require.NotContains(t, string(defaultContent), "warm debug fallback")
	require.Contains(t, string(debugContent), "warm debug fallback")
	require.NotContains(t, string(debugContent), "warm default fallback")

	fixture.unblock()
	// 解除前に完了しなかったことは、待機をsynctestで観測する
	// TestLoggerSetColdRoleCallersWaitForItsConstructionで確認します。
	receiveForTest(t, errorFallbackDone, "cold role fallback after construction")
	requireColdErrorRoleSharedForTest(t, fixture, results, concurrentLookups)
}

// TestLoggerSetColdRoleCallersWaitForItsConstructionは、未生成roleの初回構築中に同じroleを
// 求めたlookupとfallbackが、構築の完了を待ってから一つのinstanceを使うことを確認します。
// 全ての呼び出しが構築のlockを待つまでsynctest.Waitで進めてから、未完了であることを確認します。
func TestLoggerSetColdRoleCallersWaitForItsConstruction(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		fixture := newColdErrorRoleFixtureForTest(t, boundedwait.BubbleTimeout)
		const concurrentLookups = 8
		results, errorFallbackDone := startColdErrorRoleCallersForTest(t, fixture, concurrentLookups)
		// error roleのfallbackと同じroleのlookupが、構築中のroleのlockを待つまで進めてから、
		// 未完了であることを確認します。
		synctest.Wait()
		require.Empty(t, errorFallbackDone, "cold role fallback completed before construction")
		require.Empty(t, results, "same-role lookup completed before construction")

		fixture.unblock()
		require.True(t, receiveForTest(t, errorFallbackDone, "cold role fallback after construction"),
			"cold role fallback completed before construction")
		requireColdErrorRoleSharedForTest(t, fixture, results, concurrentLookups)
	})
}

func TestLoggerSetCloseIncludesUnpublishedColdRole(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		ctx := context.Background()
		cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "close-cold-role")
		cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir
		set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		entered, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		// releasedは構築の待機を解除した後だけtrueです。Closeが戻った時点で読み、解除前に戻ったかを判定します。
		var released atomic.Bool
		unblock := func() { releaseOnce.Do(func() { released.Store(true); close(release) }) }
		// 構築の待機を解除してから、上限付きで閉じます。
		t.Cleanup(func() {
			unblock()
			closeWithinBoundForTest(t, boundedwait.BubbleTimeout, "set Close", set, true)
		})
		require.NoError(t, set.ConfigureInternalHandlers(func(role InternalHandlerRole) InternalHandlers {
			if role == InternalHandlerRole(ErrorLogger) {
				close(entered)
				<-release
			}
			return InternalHandlers{}
		}))
		defaultLogger, err := set.NewFileLogger(ctx)
		require.NoError(t, err)
		type result struct {
			logger *FileLogger
			err    error
		}
		opened := make(chan result, 1)
		go func() {
			logger, err := set.NewErrorFileLogger(ctx)
			opened <- result{logger: logger, err: err}
		}()
		receiveForTest(t, entered, "cold error construction start")

		// mapへ未公開のconstructorをCloseが取りこぼして、leaseだけ解放してはいけません。
		type closeResult struct {
			err          error
			afterRelease bool
		}
		closeDone := make(chan closeResult, 1)
		go func() {
			err := set.Close()
			closeDone <- closeResult{err: err, afterRelease: released.Load()}
		}()
		// Closeが受付を停止し、未公開roleの構築が保持するlockを待つまで進めてから、完了待ちを確認します。
		synctest.Wait()
		set.mu.Lock()
		closeState := set.closeState
		set.mu.Unlock()
		require.Equal(t, common.LoggerCloseStateStarted, closeState)
		require.Empty(t, closeDone, "Close returned before unpublished construction finished")
		unblock()
		got := receiveForTest(t, opened, "accepted cold construction")
		require.NoError(t, got.err)
		errorLogger := got.logger
		closed := receiveForTest(t, closeDone, "LoggerSet Close after cold construction")
		require.NoError(t, closed.err)
		require.True(t, closed.afterRelease, "Close returned before unpublished construction finished")
		require.NotNil(t, errorLogger)
		require.Nil(t, defaultLogger.Fsstore.File)
		require.Nil(t, errorLogger.Fsstore.File)
		errorLogger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
			require.NotContains(t, protected, defaultLogger.Fsstore.ActiveFilePath)
			require.NotContains(t, protected, errorLogger.Fsstore.ActiveFilePath)
		})
		_, err = set.NewFileLogger(ctx)
		require.ErrorIs(t, err, ErrLoggerSetClosed)
		require.ErrorIs(t, errorLogger.HandleLogEntry(ctx, &logging.LogEntry{
			Level: pkglogger.LogLevelError, Message: "write after owner close",
		}), pkglogger.ErrLoggerClosed)
		// 未公開だったroleのfile/tokenも閉じた後なら、次のownerがdirectoryを取得できます。
		replacement, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		require.NoError(t, closeBoundedForTest(t, boundedwait.BubbleTimeout, "replacement Close", replacement))
	})
}
