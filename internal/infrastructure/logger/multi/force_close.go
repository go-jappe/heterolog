package multi

import (
	"errors"

	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

// ForceCloseは新規受付を止め、childへ強制終了を転送します。
// Closeの待機用mutexを取らず、実行中のchildが使うresourceの終了は通常のCloseへ委ねます。
func (m *MultiLogger) ForceClose() error {
	if m == nil {
		return nil
	}
	m.stateMu.Lock()
	if m.closeState == loggercommon.LoggerCloseStateCompleted {
		m.stateMu.Unlock()
		return nil
	}
	m.closeState = loggercommon.LoggerCloseStateStarted
	children := append([]infralogging.BackendLogEntryHandler(nil), m.loggers...)
	m.stateMu.Unlock()
	return forceCloseChildren(children)
}

// ForceCloseは非同期処理の完了を待たず受付を止め、childの復旧処理へ中止を要求します。
func (m *AsyncBestEffortMultiLogger) ForceClose() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	if m.closeState == loggercommon.LoggerCloseStateCompleted {
		m.mu.Unlock()
		return nil
	}
	m.closeState = loggercommon.LoggerCloseStateStarted
	children := append([]infralogging.BackendLogEntryHandler(nil), m.loggers...)
	m.mu.Unlock()
	return forceCloseChildren(children)
}

// ForceCloseはDelivery受付を止め、childの受理済み処理へ明示的な中止を要求します。
func (m *DeliveryMultiLogger) ForceClose() error {
	if m == nil {
		return nil
	}
	m.stateMu.Lock()
	if m.closeState == loggercommon.LoggerCloseStateCompleted {
		m.stateMu.Unlock()
		return nil
	}
	m.closeState = loggercommon.LoggerCloseStateStarted
	children := append([]infralogging.DeliveryBackendLogEntryHandler(nil), m.loggers...)
	m.stateMu.Unlock()
	return forceCloseChildren(children)
}

func forceCloseChildren[T any](children []T) error {
	errs := []error{pkgshutdown.ErrForcedClose}
	for _, child := range children {
		if err := loggercommon.ForceCloseIfSupported(child); err != nil {
			errs = append(errs, wrapHandlerErr(child, "ForceClose", err))
		}
	}
	return errors.Join(errs...)
}
