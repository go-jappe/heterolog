package fsarchivehandler

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
)

// shutdownStateは、failure処理の受付可否とShutdownの進行状態を表します。
// Started中は新規処理を拒否し、終了workerが受理済み処理と復旧の完了を待ちます。
type shutdownState uint8

const (
	// shutdownStateAcceptingは、failure処理を受け付ける通常状態です。
	shutdownStateAccepting shutdownState = iota
	// shutdownStateStartedは、Shutdown開始後かつresourceの終了処理が未完了の状態です。
	shutdownStateStarted
	// shutdownStateCompletedは、所有するresourceの終了処理がすべて完了した状態です。
	shutdownStateCompleted
)

// FailureArchivePipelineは、failure archiveの書き込み、周期保守、終了処理を
// 一つのComposite childとして所有します。FsArchiveHandlerに受付済みfailureの保存を
// 委譲し、runner modeでは保守runtimeも所有します。外部へはPipelineだけを渡し、
// Shutdown開始後に内側のhandlerを直接呼び出して受付制御を迂回できないようにします。
type FailureArchivePipeline struct {
	// handlerはfailure recordの書き込みとinline保守を担当します。
	// private fieldとし、全要求をPipelineの受付制御に通します。
	handler *FsArchiveHandler

	// runnerRuntimeはrunner modeだけで、retryable/permanent両directoryの
	// 周期保守と受理済み処理・復旧の完了待機を所有します。
	runnerRuntime *failureArchiveRunnerRuntime
	// runnerNotificationsはrunner modeのinternal log通知を、Runnerと復旧処理とは
	// 別のgoroutineで受理順に実行します。一回分のReportRunの通知はまとめて登録します。
	// Shutdownは受理済み通知の出力完了も待ちます。通知先から終了を要求する場合は
	// 自身の完了を待たないRequestShutdownを使います。
	runnerNotifications *notification.Dispatcher
	// uploadFailureRoutesはinline系列の圧縮ファイル退避・再送・purgeの利用登録です。
	// 受理済みのファイル処理とcheckpoint保存が完了してから解除します。
	uploadFailureRoutes []logfilelifecycle.ObjectStorageUploadFailureRouteOwner
	// archiveRootLeaseはretryable/permanentの共通親directoryを、Pipelineの構築から
	// Shutdown完了まで他の独立したfilesystem ownerに使用させないprocess-local leaseです。
	archiveRootLease *directorylease.Lease

	// operationMuは受付状態と処理件数だけを保護します。保存・upload中は保持せず、
	// Shutdownが新規受付を停止してから、期限付きで受理済み処理を待てるようにします。
	operationMu sync.Mutex
	// operationCountはtryAcceptFailureRequestで増やし、通知を含む要求の終了時に
	// finishFailureRequestで減らします。
	// operationDone、shutdownState、shutdownDoneとともにoperationMuで保護します。
	operationCount int
	// operationDoneはShutdownが受理済み処理の完了を待つときだけ作り、最後の完了で閉じます。
	operationDone chan struct{}
	// shutdownStateは、受付中、Shutdown開始済み、終了完了のいずれかを保持します。
	shutdownState shutdownState
	// shutdownDoneは一度だけ起動する終了workerの完了で閉じます。後発呼出しも
	// 同じchannelを自分のcontext期限まで待ち、worker自体は中止しません。
	shutdownDone chan struct{}
	// shutdownErrは終了workerの結果です。shutdownDoneの完了後、待機中の呼出しへ返します。
	shutdownErr     error
	closeController shutdown.Controller
}

// HandleFailureはPipelineが新規処理を受付中であることを確認し、
// 受付済みの要求をFsArchiveHandlerへ委譲します。
func (f *FailureArchivePipeline) HandleFailure(ctx context.Context, result loggerfailure.SendResult) error {
	if f == nil || f.handler == nil {
		return errors.New("failure archive handler is nil")
	}
	if !f.tryAcceptFailureRequest() {
		return ErrFailureArchiveClosing
	}
	defer f.finishFailureRequest()
	return f.handler.handleFailure(ctx, result)
}

// tryAcceptFailureRequestは、終了中・強制終了済みならfailure処理の要求を拒否します。
// 受付可能なら実行中の件数へ登録し、trueを返します。Shutdownとの受付競合を防ぐため、
// 受付状態の確認と件数の登録を同じlockで行います。
func (f *FailureArchivePipeline) tryAcceptFailureRequest() bool {
	if f.closeController.IsForced() {
		return false
	}
	f.operationMu.Lock()
	defer f.operationMu.Unlock()
	if f.shutdownState != shutdownStateAccepting {
		return false
	}
	f.operationCount++
	return true
}

// finishFailureRequestは、受理済みのfailure要求を実行中の件数から外し、
// 終了待ち中なら最後の要求の完了を通知します。受付成功時にdeferで登録し、
// 通知を含む要求全体の終了時に、panicの場合も一度だけ呼んでください。
func (f *FailureArchivePipeline) finishFailureRequest() {
	f.operationMu.Lock()
	defer f.operationMu.Unlock()
	f.operationCount--
	if f.operationCount == 0 && f.operationDone != nil {
		close(f.operationDone)
		f.operationDone = nil
	}
}

// ActiveArchivePathは、指定されたfailure分類へ現在書き込んでいるfile pathを返します。
// FsArchiveHandlerやFileStoreの変更可能な状態は公開せず、稼働状態の観測に必要なpathだけを
// 提供します。Pipelineが未初期化、分類が未知、またはactive fileがない場合はerrorを返し、
// 空文字を状態表現として使用しません。
func (f *FailureArchivePipeline) ActiveArchivePath(
	failureCategory failurearchive.FailureCategory,
) (string, error) {
	if f == nil || f.handler == nil || f.handler.Fsstore == nil {
		return "", errors.New("failure archive pipeline is not initialized")
	}
	f.handler.Fsstore.Mu.Lock()
	defer f.handler.Fsstore.Mu.Unlock()

	var (
		path   string
		active bool
	)
	switch failureCategory {
	case failurearchive.FailureCategoryRetryable:
		path = f.handler.Fsstore.ActiveFilePathRetryable
		active = f.handler.Fsstore.FileRetryable != nil
	case failurearchive.FailureCategoryPermanent:
		path = f.handler.Fsstore.ActiveFilePathPermanent
		active = f.handler.Fsstore.FilePermanent != nil
	default:
		return "", fmt.Errorf("unsupported failure archive category: %q", failureCategory)
	}
	if !active || path == "" {
		return "", fmt.Errorf("active %s failure archive file is not open", failureCategory)
	}
	return path, nil
}
