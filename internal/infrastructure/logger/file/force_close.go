package file

import (
	"context"
	"sync"

	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
)

// fileForceContextはcallerのtimeoutと分離した、明示的な処理中止の通知です。
type fileForceContext struct {
	once      sync.Once
	abortOnce sync.Once
	done      chan struct{}
}

func (f *fileForceContext) initialize() {
	f.once.Do(func() { f.done = make(chan struct{}) })
}

func (f *fileForceContext) context(parent context.Context) (context.Context, context.CancelFunc) {
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

func (f *fileForceContext) abort() {
	f.initialize()
	f.abortOnce.Do(func() { close(f.done) })
}

// 集合配下でも中止は即座に確認し、非同期の資源解放開始を待って新規writeを許可しません。
func (l *FileLogger) forceRequested() bool {
	owner := l.ownerCloseController.Load()
	return l.closeController.IsForced() || owner != nil && owner.IsForced()
}

func (l *DeliveryFileLogger) forceRequested() bool {
	owner := l.ownerCloseController.Load()
	return l.closeController.IsForced() || owner != nil && owner.IsForced()
}

// ForceCloseは完了保証を放棄し、進行中のClose呼出しも明示的な中止errorで返します。
// 実処理がcontextを無視する場合も、fileとleaseは処理が戻るまで保持します。
func (l *FileLogger) ForceClose() error {
	if l == nil || l.managedByLoggerSet {
		return nil
	}
	err := l.RequestForceClose()
	go l.Close()
	return err
}

// RequestForceCloseは中止だけを通知し、親が管理する資源解放の順序を変更しません。
func (l *FileLogger) RequestForceClose() error {
	if l == nil || l.managedByLoggerSet {
		return nil
	}
	return l.closeController.ForceClose(l.forceOwnedFileResource)
}

// ShutdownDoneは実際の終了workerが戻るまで待つ親所有者向けの通知です。
// 資源解放の成否はShutdownCompleteで確認します。
func (l *FileLogger) ShutdownDone() <-chan struct{}         { return l.closeController.Done() }
func (l *DeliveryFileLogger) ShutdownDone() <-chan struct{} { return l.closeController.Done() }
func (s *LoggerSet) ShutdownDone() <-chan struct{}          { return s.closeController.Done() }
func (s *DeliveryLoggerSet) ShutdownDone() <-chan struct{}  { return s.closeController.Done() }

func (l *FileLogger) forceOwnedFileResource() {
	l.forceWork.abort()
	_ = l.archiveLogFileProcessingRecovery.ForceClose()
	if force, ok := l.archiveUploadCheckpoints.(interface{ ForceClose() error }); ok {
		_ = force.ForceClose()
	}
	l.backgroundMu.Lock()
	archiveUploader := l.ArchiveUploader
	l.backgroundMu.Unlock()
	go func() { _ = loggercommon.ForceCloseIfSupported(archiveUploader) }()
}

// ShutdownCompleteは終了時errorの有無とは別に、所有資源の解放完了を返します。
func (l *FileLogger) ShutdownComplete() bool {
	if l == nil || l.managedByLoggerSet {
		return true
	}
	l.backgroundMu.Lock()
	defer l.backgroundMu.Unlock()
	return l.closeState == loggercommon.LoggerCloseStateCompleted
}

func (l *DeliveryFileLogger) ShutdownComplete() bool {
	if l == nil || l.managedByDeliveryLoggerSet {
		return true
	}
	l.lifecycleMu.Lock()
	defer l.lifecycleMu.Unlock()
	return l.closeState == loggercommon.LoggerCloseStateCompleted
}

func (s *LoggerSet) ShutdownComplete() bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeState == loggercommon.LoggerCloseStateCompleted
}

func (s *DeliveryLoggerSet) ShutdownComplete() bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeState == loggercommon.LoggerCloseStateCompleted
}
