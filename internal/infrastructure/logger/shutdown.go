package logger

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

func (l *topLevelLoggerRuntime) Close() error {
	if l == nil {
		return nil
	}
	l.topLevelLoggerDelegate.StopAccepting()
	return l.closeController.Close("logger", func() (bool, error) {
		err := l.closeAttempt()
		l.closeMu.Lock()
		complete := l.closeStage == topLevelLoggerCloseStageCompleted
		l.closeMu.Unlock()
		return complete, err
	})
}

func (l *topLevelAsyncLoggerRuntime) Close() error {
	if l == nil {
		return nil
	}
	l.topLevelAsyncLoggerDelegate.StopAccepting()
	return l.closeController.Close("async logger", func() (bool, error) {
		err := l.closeAttempt()
		l.closeMu.Lock()
		complete := l.closeStage == topLevelLoggerCloseStageCompleted
		l.closeMu.Unlock()
		return complete, err
	})
}

func (l *topLevelDeliveryLoggerRuntime) Close() error {
	if l == nil {
		return nil
	}
	l.topLevelDeliveryLoggerDelegate.StopAccepting()
	return l.closeController.Close("delivery logger", func() (bool, error) {
		err := l.closeAttempt()
		l.closeMu.Lock()
		complete := l.closeStage == topLevelLoggerCloseStageCompleted
		l.closeMu.Unlock()
		return complete, err
	})
}

// ForceCloseは全所有者へ中止を要求します。一つの資源の中止待ちで他の通知を止めません。
// 未完了処理が使う資源は通常の終了経路で、その処理が戻ってから解放します。
func (l *topLevelLoggerRuntime) ForceClose() error {
	if l == nil {
		return nil
	}
	l.topLevelLoggerDelegate.StopAccepting()
	return l.closeController.ForceClose(func() {
		requestForcedClose(l.topLevelLoggerDelegate, l.appendLogMaintenanceRuntime, l.failureSpoolPipelines,
			l.fallbackCloserOwner, l.internalLogDestinations, l.fileLoggerSet, l.compressedLogSpoolRuntimes)
		_ = l.Close()
	})
}

func (l *topLevelAsyncLoggerRuntime) ForceClose() error {
	if l == nil {
		return nil
	}
	l.topLevelAsyncLoggerDelegate.StopAccepting()
	return l.closeController.ForceClose(func() {
		requestForcedClose(l.topLevelAsyncLoggerDelegate, l.appendLogMaintenanceRuntime, l.failureSpoolPipelines,
			l.fallbackCloserOwner, l.internalLogDestinations, l.fileLoggerSet, l.compressedLogSpoolRuntimes)
		_ = l.Close()
	})
}

func (l *topLevelDeliveryLoggerRuntime) ForceClose() error {
	if l == nil {
		return nil
	}
	l.topLevelDeliveryLoggerDelegate.StopAccepting()
	return l.closeController.ForceClose(func() {
		requestForcedClose(l.topLevelDeliveryLoggerDelegate, l.appendLogMaintenanceRuntime,
			l.fallbackCloserOwner, l.internalLogDestinations, l.fileLoggerSet, l.compressedLogSpoolRuntimes)
		_ = l.Close()
	})
}

func requestForcedClose(owners ...any) {
	for _, owner := range owners {
		switch owner.(type) {
		case interface{ RequestForceClose() error }, interface{ ForceClose() error }:
			go func() {
				if err := common.ForceCloseIfSupported(owner); err != nil && !errors.Is(err, pkgshutdown.ErrForcedClose) {
					_, _ = fmt.Fprintf(os.Stderr, "force shutdown notification: %v\n", err)
				}
			}()
		}
	}
}

// retainCompletedCloseErrorは後片付けが完了済みのエラーを報告用に残します。
// 未完了の失敗だけを終了処理の再試行対象にし、閉じた資源を繰り返し閉じません。
func retainCompletedCloseError(owner any, err error, retained *[]error) error {
	if err == nil {
		return nil
	}
	common.WaitForForcedShutdown(owner, err)
	if state, ok := owner.(interface{ ShutdownComplete() bool }); ok && state.ShutdownComplete() {
		*retained = append(*retained, err)
		return nil
	}
	return err
}

func (r *internalLogDestinationRuntime) ShutdownComplete() bool {
	return r == nil || r.runtimeOwnedFileLoggerSet == nil || r.runtimeOwnedFileLoggerSet.ShutdownComplete()
}

func (r *deliveryInternalLogDestinationRuntime) ShutdownComplete() bool {
	return r == nil || r.runtimeOwnedFileLoggerSet == nil || r.runtimeOwnedFileLoggerSet.ShutdownComplete()
}

func (r *appendLogMaintenanceRuntime) ForceClose() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	r.closed = true
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	for _, entry := range r.entries {
		requestForcedClose(entry.runner)
	}
	return pkgshutdown.ErrForcedClose
}

func (r *internalLogDestinationRuntime) ForceClose() error {
	if r == nil {
		return nil
	}
	requestForcedClose(r.runtimeOwnedFileLoggerSet)
	return pkgshutdown.ErrForcedClose
}

func (r *deliveryInternalLogDestinationRuntime) ForceClose() error {
	if r == nil {
		return nil
	}
	requestForcedClose(r.runtimeOwnedFileLoggerSet)
	return pkgshutdown.ErrForcedClose
}

func (o *fallbackHandlerCloserOwner) ForceClose() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	closers := make([]io.Closer, 0, len(o.closers))
	for _, state := range o.closers {
		closers = append(closers, state.closer)
	}
	o.mu.Unlock()
	for _, closer := range closers {
		requestForcedClose(closer)
	}
	return pkgshutdown.ErrForcedClose
}

func (s *failureSpoolPipelineSet) ForceClose() error {
	if s == nil {
		return nil
	}
	s.registrationMu.Lock()
	s.registrationSealed = true
	writers := append([]*failureSpoolOwnedWriter(nil), s.writers...)
	s.registrationMu.Unlock()
	for _, writer := range writers {
		requestForcedClose(writer.handler)
	}
	requestForcedClose(s.runtime)
	return pkgshutdown.ErrForcedClose
}

// ForceCloseはspoolの周期処理へ中止を要求します。保存済みの再送待ちデータは残します。
func (r *FailureSpoolRuntime) ForceClose() error {
	if r == nil {
		return nil
	}
	err := r.RequestForceClose()
	go func() { _ = r.Close() }()
	return err
}

func (r *compressedLogSpoolRuntimes) ForceClose() error {
	if r == nil {
		return nil
	}
	// 各rootの受理済み処理へ中止を伝えます。通常Closeと異なり、その完了を待ちません。
	for _, runtime := range r.runtimes {
		requestForcedClose(runtime)
	}
	go func() { _ = r.Close() }()
	return pkgshutdown.ErrForcedClose
}
