package cloudwatchlogs

import (
	"errors"
	"log"

	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
)

// RequestForceCloseは遅延生成済みloggerへ中止だけを転送します。
// 初期化中でも受付は直ちに停止し、実際の資源解放は親のClose順序に従います。
func (h *fallbackHandlerController) RequestForceClose() error {
	if h == nil {
		return nil
	}
	if !h.forceRequested.Swap(true) {
		go func() {
			h.mu.Lock()
			h.closing = true
			logger := h.logger
			h.mu.Unlock()
			if logger == nil {
				return
			}
			if err := loggercommon.ForceCloseIfSupported(logger); err != nil && !errors.Is(err, shutdown.ErrForcedClose) {
				log.Printf("force fallback logger shutdown: %v", err)
			}
		}()
	}
	return shutdown.ErrForcedClose
}

// ForceCloseは内部ownerの中止契約です。資源のCloseは独立して開始しません。
func (h *fallbackHandlerController) ForceClose() error { return h.RequestForceClose() }

// ShutdownCompleteはwrapper自身の受理済み処理と所有loggerの終了完了を返します。
func (h *fallbackHandlerController) ShutdownComplete() bool {
	if h == nil {
		return true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}

// ForceCloseは受付を停止し、flush managerの復旧処理へ強制終了を転送します。
// 実行中の送信やCloseが保持するmutexは待たず、resource解放は通常のCloseへ委ねます。
func (l *CloudWatchLogsLogger) ForceClose() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	if l.closeState == loggercommon.LoggerCloseStateCompleted {
		l.mu.Unlock()
		return nil
	}
	l.closeState = loggercommon.LoggerCloseStateStarted
	manager := l.flushManager
	l.mu.Unlock()
	return errors.Join(shutdown.ErrForcedClose, loggercommon.ForceCloseIfSupported(manager))
}

// ShutdownCompleteは送信結果のエラーとは独立して所有する処理の終了完了を返します。
func (l *CloudWatchLogsLogger) ShutdownComplete() bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closeState == loggercommon.LoggerCloseStateCompleted
}

// ForceCloseは受理済みfailure処理の復旧を中止します。送信clientの資源は閉じません。
func (fm *CloudWatchLogsFlushManager) ForceClose() error {
	if fm == nil {
		return nil
	}
	return errors.Join(shutdown.ErrForcedClose, fm.FailureHandler.ForceClose())
}

// ShutdownCompleteはfailure handlerの実際の資源解放完了を返します。
func (fm *CloudWatchLogsFlushManager) ShutdownComplete() bool {
	return fm == nil || fm.FailureHandler == nil || fm.FailureHandler.ShutdownComplete()
}
