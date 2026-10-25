package fsarchivehandler

import (
	"context"
	"sync"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
)

// archiveForceContextはcallerのtimeoutから独立した明示的な中止通知を保持します。
type archiveForceContext struct {
	once      sync.Once
	abortOnce sync.Once
	done      chan struct{}
}

func (f *archiveForceContext) initialize() {
	f.once.Do(func() { f.done = make(chan struct{}) })
}

func (f *archiveForceContext) context(parent context.Context) (context.Context, context.CancelFunc) {
	f.initialize()
	ctx, cancel := context.WithCancel(parent)
	select {
	case <-f.done:
		cancel()
	default:
		go func() {
			select {
			case <-f.done:
				cancel()
			case <-ctx.Done():
			}
		}()
	}
	return ctx, cancel
}

func (f *archiveForceContext) abort() {
	f.initialize()
	f.abortOnce.Do(func() { close(f.done) })
}

// ForceCloseは新規受付と受理済み処理の待機を停止し、完了保証を放棄したerrorを返します。
// 使用中のfile・checkpoint・route・leaseの解放は、終了workerが処理終了後に行います。
func (f *FailureArchivePipeline) ForceClose() error {
	if f == nil {
		return nil
	}
	err := f.RequestForceClose()
	go f.Shutdown(context.Background())
	return err
}

// RequestForceCloseは親の終了順序を追い越さず、中止だけを通知します。
func (f *FailureArchivePipeline) RequestForceClose() error {
	if f == nil {
		return nil
	}
	return f.closeController.ForceClose(func() {
		f.runnerRuntime.stopPeriodic()
		if f.handler != nil {
			f.handler.forceWork.abort()
			if recovery := f.handler.logFileProcessingRecovery; recovery != nil {
				_ = recovery.ForceClose()
			}
			for _, category := range []failurearchive.FailureCategory{failurearchive.FailureCategoryRetryable, failurearchive.FailureCategoryPermanent} {
				if force, ok := f.handler.compressedArchiveUploadCheckpointStore(category).(interface{ ForceClose() error }); ok {
					_ = force.ForceClose()
				}
			}
		}
		if f.runnerRuntime != nil {
			for _, runner := range f.runnerRuntime.runners {
				if runner != nil {
					_ = runner.ForceClose()
				}
			}
		}
		for _, owner := range f.uploadFailureRoutes {
			_ = owner.RequestForceClose()
		}
	})
}

// ShutdownDoneは終了workerが実行中の処理と資源の終了を待ち終えた通知です。
func (f *FailureArchivePipeline) ShutdownDone() <-chan struct{} { return f.closeController.Done() }

// ShutdownCompleteは終了結果と独立に、所有資源の後片付けが終わったことを返します。
func (f *FailureArchivePipeline) ShutdownComplete() bool {
	if f == nil {
		return true
	}
	f.operationMu.Lock()
	defer f.operationMu.Unlock()
	return f.shutdownState == shutdownStateCompleted
}
