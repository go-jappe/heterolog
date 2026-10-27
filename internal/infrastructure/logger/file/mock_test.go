package file

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	commoncheckpoint "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	fileerrors "github.com/go-jappe/heterolog/internal/infrastructure/logger/file/errors"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

	"github.com/stretchr/testify/require"
)

// standaloneの試験でも、アプリ起動時と同様にcheckpoint保存先を先に準備します。
func newStandaloneFileLogger(
	ctx context.Context,
	cfg FileLoggerConfigProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (*FileLogger, error) {
	if cfg != nil && cfg.ArchiveCheckpoint().Directory != "" {
		if err := commoncheckpoint.PrepareDirectory(cfg.ArchiveCheckpoint().Directory); err != nil {
			return nil, err
		}
	}
	return newFileLoggerBase(ctx, cfg, DefaultLogger, InternalHandlers{}, false, tracer, meter, nil)
}

func newStandaloneErrorFileLogger(
	ctx context.Context,
	cfg FileLoggerConfigProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (*FileLogger, error) {
	if cfg != nil && cfg.ArchiveCheckpoint().Directory != "" {
		if err := commoncheckpoint.PrepareDirectory(cfg.ArchiveCheckpoint().Directory); err != nil {
			return nil, err
		}
	}
	return newFileLoggerBase(ctx, cfg, ErrorLogger, InternalHandlers{}, false, tracer, meter, nil)
}

func newStandaloneDebugFileLogger(
	ctx context.Context,
	cfg FileLoggerConfigProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (*FileLogger, error) {
	if cfg != nil && cfg.ArchiveCheckpoint().Directory != "" {
		if err := commoncheckpoint.PrepareDirectory(cfg.ArchiveCheckpoint().Directory); err != nil {
			return nil, err
		}
	}
	return newFileLoggerBase(ctx, cfg, DebugLogger, InternalHandlers{}, false, tracer, meter, nil)
}

func openFileLoggerForTest(ctx context.Context, logger *FileLogger) error {
	logger.Fsstore.Mu.Lock()
	defer logger.Fsstore.Mu.Unlock()
	return logger.openLogFile(ctx, logger.Clock.Now())
}

func openDeliveryFileLoggerForTest(ctx context.Context, logger *DeliveryFileLogger) error {
	logger.Fsstore.Mu.Lock()
	defer logger.Fsstore.Mu.Unlock()
	return logger.openLogFile(ctx, logger.Clock.Now())
}

func purgeFileLoggerForTest(ctx context.Context, logger *FileLogger) error {
	logger.Fsstore.Mu.Lock()
	fileNameSnapshot := logger.Fsstore.FileName
	logger.Fsstore.Mu.Unlock()
	inlinePurge := logger.inlinePurge
	if inlinePurge == nil {
		return nil
	}
	plan := logger.planInlineCleanup()
	if !plan.cleanupRequired() {
		return nil
	}
	return logger.cleanupOldLogs(
		ctx,
		fileNameSnapshot,
		plan,
	)
}

func rotateFileLoggerForTest(ctx context.Context, logger *FileLogger) error {
	// production writeと同じ順序でClose受付、registration予約、file lockを取得します。
	if err := logger.lockForWrite(); err != nil {
		return err
	}
	registrationPending := true
	finishRegistration := func() {
		if !registrationPending {
			return
		}
		registrationPending = false
		logger.endBackgroundOperation()
	}
	defer func() {
		finishRegistration()
		logger.Fsstore.Mu.Unlock()
	}()

	err := logger.rotateWithHandlers(
		ctx,
		logger.Clock.Now(),
		logger.wrapAndDispatchError,
		logger.InternalErrorEntryWithContext,
	)
	finishRegistration()
	return err
}

// fillActiveFileLoggerForRotationは、既存の追記専用handleでrotation上限まで埋めます。
// WindowsではこのhandleのTruncateが許可されないため、長さ変更ではなく実際に追記します。
// writer lock内でpathとサイズを確定し、呼出し側が次の通常write/Rotateを実行できます。
func fillActiveFileLoggerForRotation(t *testing.T, logger *FileLogger) string {
	t.Helper()
	logger.Fsstore.Mu.Lock()
	defer logger.Fsstore.Mu.Unlock()
	info, err := logger.Fsstore.File.Stat()
	if err != nil {
		t.Fatal(err)
	}
	remaining := int64(logger.config.Fsstore().Lifecycle().MaxFileSize()) - info.Size()
	// 大きな閾値でもfixtureの一時メモリは32 KiBに制限します。
	padding := make([]byte, 32*1024)
	for remaining > 0 {
		chunkSize := min(remaining, int64(len(padding)))
		written, err := logger.Fsstore.File.Write(padding[:chunkSize])
		if err != nil {
			t.Fatal(err)
		}
		if written != int(chunkSize) {
			t.Fatalf("fill active log: wrote %d bytes, want %d", written, chunkSize)
		}
		remaining -= int64(written)
	}
	return logger.Fsstore.ActiveFilePath
}

// synctestBubbleHangTimeoutForTestは、synctestのbubble一つが実時間で終わるまで待つ上限です。
// bubble内の待機の上限は仮想時間で数えます。しかしbubble内のgoroutineが、synctestが停止状態と
// みなさない待機（停止したgoroutineが保持し続けるsync.Mutexなど）で止まると、仮想時間が進まず
// その上限は働きません。そうした退行もgo testの全体のtimeoutを待たずに失敗させるための上限です。
// bubble内の処理は実時間では短く（-raceでも数秒）、正常な実行では到達しないよう十分長くします。
const synctestBubbleHangTimeoutForTest = 2 * time.Minute

// synctestWithHangWatchdogForTestは、fをsynctestのbubble内で実行します。bubbleが
// synctestBubbleHangTimeoutForTest以内に終わらなければ、理由と全goroutineのstackを出力して
// 試験のprocessを停止させます。止まったbubbleを中から失敗させる手段はないため、bubbleの外で
// 実時間を数える監視goroutineから中止します。
func synctestWithHangWatchdogForTest(t *testing.T, f func(*testing.T)) {
	t.Helper()
	finished := make(chan struct{})
	defer close(finished)
	// bubbleの外で起動するため、監視goroutineのtimerは実時間で数えます。
	go watchSynctestBubbleForTest(t.Name(), finished)
	synctest.Test(t, f)
}

// watchSynctestBubbleForTestは、finishedが閉じられないままsynctestBubbleHangTimeoutForTestが
// 過ぎたら、go testのtimeoutと同じく全goroutineのstackを出力して試験のprocessを停止させます。
func watchSynctestBubbleForTest(name string, finished <-chan struct{}) {
	timer := time.NewTimer(synctestBubbleHangTimeoutForTest)
	defer timer.Stop()
	select {
	case <-finished:
	case <-timer.C:
		debug.SetTraceback("all")
		panic(fmt.Sprintf("%s: the synctest bubble did not finish within %s of real time; "+
			"a goroutine in the bubble is probably blocked on a lock that synctest cannot observe "+
			"(such as a sync.Mutex held by a blocked goroutine)", name, synctestBubbleHangTimeoutForTest))
	}
}

// synctestFromWallClockは、synctestの仮想時計を開始時点の実時刻まで進めてからfを実行します。
// Clockを固定する前に作られるactive fileの日付を実時間で実行した場合と揃え、
// 固定日付のfixtureとの世代の前後関係を変えずに、再試行timerの待機だけを仮想時間で進めます。
// bubbleが実時間で終わらない場合はsynctestWithHangWatchdogForTestが試験を中止します。
func synctestFromWallClock(t *testing.T, f func(*testing.T)) {
	t.Helper()
	start := time.Now()
	synctestWithHangWatchdogForTest(t, func(t *testing.T) {
		time.Sleep(start.Sub(time.Now()))
		f(t)
	})
}

// requireStartedCloseCompletesForTestは、別goroutineで開始したCloseがerrorなしで完了することを
// 仮想時間の期限（boundedwait.BubbleTimeout）付きで待ちます。受理済みの処理が再試行を続けて
// Closeが戻らない場合は、期限後にForceCloseで止め、その戻りも同じ期限まで待ってから試験を失敗させ、
// 仮想時間を進め続けて試験が終わらなくなることを防ぎます。synctestのbubble内で使います。
func requireStartedCloseCompletesForTest(t *testing.T, logger *FileLogger, closed <-chan error) {
	t.Helper()
	timer := time.NewTimer(boundedwait.BubbleTimeout)
	defer timer.Stop()
	select {
	case err := <-closed:
		require.NoError(t, err, "Close() error")
		return
	case <-timer.C:
	}
	_ = logger.ForceClose()
	boundedwait.ReceiveWithin(t, closed, boundedwait.BubbleTimeout, "Close after ForceClose")
	t.Fatalf("Close did not complete within %s while accepted log file processing kept retrying", boundedwait.BubbleTimeout)
}

// requireArchiveRecoveryClosesForTestは、受理済みの再開処理が完了してCloseがerrorなしで戻ることを
// 仮想時間の期限（boundedwait.BubbleTimeout）付きで確認します。期限内に戻らない場合は
// ForceCloseで止めて試験を失敗させます（closeBoundedForTest）。synctestのbubble内で使います。
func requireArchiveRecoveryClosesForTest(t *testing.T, logger *FileLogger) {
	t.Helper()
	require.NoError(t, closeBoundedForTest(t, boundedwait.BubbleTimeout,
		"archive log file processing recovery Close", &logger.archiveLogFileProcessingRecovery))
}

// hangTimeoutForTestは、試験が処理の停止（hang）を検出するまで待つ上限です。正常な処理に
// 必要な時間の見積もりではなく、失敗した試験を待ち続けないための上限です。
// synctestのbubble内では仮想時間で数えます。他のpackageの試験と同じ値を使います。
const hangTimeoutForTest = boundedwait.HangTimeout

// receiveForTestは、chから値を一つ受け取ります。hangTimeoutForTest以内に受け取れなければ、
// 待ち続けずに試験を失敗させます。whatには待っている事象か、届かない場合の失敗の内容を書きます。
func receiveForTest[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	timer := time.NewTimer(hangTimeoutForTest)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		t.Fatalf("%s: nothing received within %s", what, hangTimeoutForTest)
		var zero T
		return zero
	}
}

// waitForTestは、waitが戻るまで待ちます。hangTimeoutForTest以内に戻らなければ、
// 待ち続けずに試験を失敗させます。
func waitForTest(t *testing.T, what string, wait func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		wait()
	}()
	receiveForTest[struct{}](t, done, what)
}

// waitBackgroundForTestは、loggerが受理したbackground処理の完了を、hang検出の上限付きで待ちます。
func waitBackgroundForTest(t *testing.T, logger *FileLogger) {
	t.Helper()
	waitForTest(t, "file logger background operations", logger.backgroundWG.Wait)
}

// waitForGoroutineStackForTestは、framesの文字列をすべて含むstackのgoroutineが現れるまで待ちます。
// sync.Mutexの待機はsynctestで観測できないため、処理が待機地点（例えば"sync.(*Mutex).Lock"と
// 呼出し元の関数名）に達したことをgoroutineのstackで確認します。達しなければhang検出の上限で
// 試験を失敗させます。待機地点に達した後の「まだ完了していない」確認を、時間の窓に頼らず行えます。
func waitForGoroutineStackForTest(t *testing.T, what string, frames ...string) {
	t.Helper()
	deadline := time.Now().Add(hangTimeoutForTest)
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n == len(buf) {
			buf = make([]byte, 2*len(buf))
			continue
		}
		for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
			found := true
			for _, frame := range frames {
				if !strings.Contains(stack, frame) {
					found = false
					break
				}
			}
			if found {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not reach %q within %s", what, frames, hangTimeoutForTest)
		}
		time.Sleep(time.Millisecond)
	}
}

// releaseOnCleanupForTestは、注入した待機chを一度だけ閉じる関数を返し、同じ関数をcleanupにも
// 登録します。試験が解除前に失敗しても、待機中の処理を残さずにloggerを閉じられます。
// cleanupは後に登録したものから実行されるため、対象loggerを作成した後に呼び出してください。
func releaseOnCleanupForTest(t *testing.T, ch chan struct{}) func() {
	t.Helper()
	release := sync.OnceFunc(func() { close(ch) })
	t.Cleanup(release)
	return release
}

// closerForTestは、試験の後片付けで閉じる値です。ForceCloseも持つ値は、終了処理の待機が
// 上限を過ぎた場合と試験が既に失敗している場合に、ForceCloseで中止を通知します。
type closerForTest interface{ Close() error }

// closeOnCleanupWithinForTestは、試験の終了時にcloserが返す値を上限timeoutで閉じるcleanupを
// 登録します。closerはcleanupの実行時に呼ぶため、試験の途中で変数を再代入した場合も最後の値を
// 閉じます。timeoutにはsynctestのbubble内ならboundedwait.BubbleTimeout、bubble外なら
// boundedwait.HangTimeoutを渡します。requireNoErrorがtrueなら、成功した試験でCloseが
// errorを返さないことも確認します。
func closeOnCleanupWithinForTest(t *testing.T, timeout time.Duration, description string, closer func() closerForTest, requireNoError bool) {
	t.Helper()
	t.Cleanup(func() { closeWithinBoundForTest(t, timeout, description, closer(), requireNoError) })
}

// closeBoundedForTestは、試験の本文でcloserを上限timeoutで閉じ、Closeのerrorを返します。
// 本番のCloseは受理済み処理の完了を期限なく待つ場合があるため、上限内に戻らなければ、
// ForceCloseを持つ値はForceCloseで中止を通知したうえで試験を失敗させます
// （boundedwait.CloseWithin）。試験のgoroutineから呼びます。
func closeBoundedForTest(t *testing.T, timeout time.Duration, description string, closer closerForTest) error {
	t.Helper()
	return boundedwait.CloseWithin(t, timeout, description, closer.Close, boundedwait.ForceCloserOf(closer))
}

// closeWithinBoundForTestは、cleanupの中でcloserを上限timeoutで閉じます。他の解除処理の後に
// 閉じるcleanupで使います。試験が既に失敗またはskipしていれば、残った障害注入を待たないよう
// 先にForceCloseし、Closeのerrorは確認しません。Closeが上限内に戻らなければ、ForceCloseで
// 中止を通知したうえで試験を失敗させます（boundedwait.CloseWithin）。
func closeWithinBoundForTest(t *testing.T, timeout time.Duration, description string, closer closerForTest, requireNoError bool) {
	t.Helper()
	force := boundedwait.ForceCloserOf(closer)
	if t.Failed() || t.Skipped() {
		if force != nil {
			_ = force()
		}
		_ = boundedwait.CloseWithin(t, timeout, description, closer.Close, force)
		return
	}
	err := boundedwait.CloseWithin(t, timeout, description, closer.Close, force)
	if requireNoError {
		boundedwait.NoError(t, err)
	}
}

// resolvedTempDirForTestは、symlinkを解決したt.TempDirを返します。
// loggerは保存先を実体のpathで扱うため、macOSのように一時directoryが
// symlinkを経由する環境でも、期待するpathを同じ実体のpathから組み立てます。
func resolvedTempDirForTest(t *testing.T) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return directory
}

// requireFileLoggerInternalErrorは、OnErrorへ渡されたerrorが、wantRoleのFileLoggerが
// 処理の種類wantOpで作ったrole別の内部errorであることを確認します。
func requireFileLoggerInternalError(t *testing.T, err error, wantRole loggerType, wantOp string) {
	t.Helper()
	var defaultErr *fileerrors.DefaultLoggerInternalError
	var errorErr *fileerrors.ErrorLoggerInternalError
	var debugErr *fileerrors.DebugLoggerInternalError
	var role loggerType
	var op string
	switch {
	case errors.As(err, &defaultErr):
		role, op = DefaultLogger, defaultErr.Op
	case errors.As(err, &errorErr):
		role, op = ErrorLogger, errorErr.Op
	case errors.As(err, &debugErr):
		role, op = DebugLogger, debugErr.Op
	default:
		t.Fatalf("error %v is not a file logger internal error", err)
	}
	if role != wantRole || op != wantOp {
		t.Fatalf("internal error role=%q op=%q, want role=%q op=%q: %v", role, op, wantRole, wantOp, err)
	}
}
