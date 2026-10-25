package fsarchivehandler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
)

// failureArchiveHangTimeoutは、圧縮・fsync・送信を含む非同期処理が止まったことを検出する上限です。
// 処理の順序や完了は通知で確認し、この時間は停止の検出だけに使います。
const failureArchiveHangTimeout = 10 * time.Second

// shutdownFailureArchiveForTestは、停止の検出だけに使う期限付きでShutdownを呼びます。
// 終了処理が止まった場合は期限のerrorを返し、試験を止めたままにしません。
func shutdownFailureArchiveForTest(pipeline interface{ Shutdown(context.Context) error }) error {
	ctx, cancel := context.WithTimeout(context.Background(), failureArchiveHangTimeout)
	defer cancel()
	return pipeline.Shutdown(ctx)
}

// waitFailureArchiveClosedForTestは、ForceCloseやShutdownの後の終了を、停止の検出だけに使う
// 期限まで待ちます。終わらない場合は試験を失敗にして、後続の片付けへ進みます。
func waitFailureArchiveClosedForTest(t *testing.T, pipeline *FailureArchivePipeline) {
	t.Helper()
	select {
	case <-pipeline.closeController.Done():
	case <-time.After(failureArchiveHangTimeout):
		t.Error("failure archive pipeline did not finish closing")
	}
}

// dispatchRotatedArchiveAndWaitは、ローテーション済みファイルを通常のrotation taskへ渡し、
// 圧縮・upload・削除と後続のcleanupを含むtaskの完了を待ちます。概算の無効化や
// 再走査の予約は本番のtask完了処理が行い、試験側では補いません。
func dispatchRotatedArchiveAndWait(
	t *testing.T,
	handler *FsArchiveHandler,
	activeFilePath string,
	oldFilePath string,
	failureCategory failurearchive.FailureCategory,
) {
	t.Helper()
	handler.archiveTaskMu.Lock()
	previousTask := handler.archiveTaskGeneration
	handler.archiveTaskMu.Unlock()
	handler.dispatchArchiveRotatedFile(context.Background(), activeFilePath, oldFilePath, failureCategory)
	handler.archiveTaskMu.Lock()
	taskDone := handler.archiveTaskGeneration
	handler.archiveTaskMu.Unlock()
	if taskDone == nil || taskDone == previousTask {
		t.Fatal("rotated archive did not start a rotation task")
	}
	select {
	case <-taskDone:
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("rotation task did not complete")
	}
}

// countArchiveDirectoryObservationsは、ディレクトリ全体の観測回数を数えるようにglobを置き換えます。
// 系列だけを走査するupload再試行は数えず、どちらも本番と同じ関数で列挙します。
func countArchiveDirectoryObservations(handler *FsArchiveHandler) func() int {
	var mu sync.Mutex
	observations := 0
	handler.globArchiveFiles = func(pattern logpattern.FileNamePattern, mode logpattern.GlobMode) ([]string, error) {
		if mode != logpattern.GlobAllFiles {
			return logpattern.GlobFileNames(pattern, mode)
		}
		mu.Lock()
		observations++
		mu.Unlock()
		return logpattern.GlobLifecycleFileNames(pattern, mode)
	}
	return func() int {
		mu.Lock()
		defer mu.Unlock()
		return observations
	}
}

// rotationPanicReportHandlersは、rotation taskが回復したpanicの通知だけをreportedへ送るhandlerです。
func rotationPanicReportHandlers(reported chan<- error) InternalLogHandlers {
	return InternalLogHandlers{OnError: func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
		var internalErr *FailureArchiveInternalError
		if errors.As(err, &internalErr) && internalErr.Op == OperationRotationArchive {
			select {
			case reported <- err:
			default:
			}
		}
	}}
}

// requireRotationPanicReportedは、rotation taskのpanicが値を含めて通知済みであることを確認します。
// taskの完了後に呼び、通知が届いていない場合は待たずに失敗します。
func requireRotationPanicReported(t *testing.T, reported <-chan error, panicValue string) {
	t.Helper()
	select {
	case err := <-reported:
		var panicErr *ArchiveTaskPanicError
		if !errors.As(err, &panicErr) || panicErr.Value != panicValue {
			t.Fatalf("rotation task panic notification = %v, want recovered value %q", err, panicValue)
		}
	default:
		t.Fatal("rotation task panic was not reported")
	}
}
