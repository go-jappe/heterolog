package multi

import loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"

func childShutdownComplete(child any) bool {
	completed, ok := child.(interface{ ShutdownComplete() bool })
	return ok && completed.ShutdownComplete()
}

func allChildrenClosed(children []bool) bool {
	for _, closed := range children {
		if !closed {
			return false
		}
	}
	return true
}

// ShutdownCompleteは確定した終了エラーの有無とは独立してchildの資源解放完了を返します。
func (m *MultiLogger) ShutdownComplete() bool {
	if m == nil {
		return true
	}
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return m.closeState == loggercommon.LoggerCloseStateCompleted
}

// ShutdownCompleteは非同期処理とすべてのchildの終了が完了したことを返します。
func (m *AsyncBestEffortMultiLogger) ShutdownComplete() bool {
	if m == nil {
		return true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeState == loggercommon.LoggerCloseStateCompleted
}

// ShutdownCompleteはDelivery処理とすべてのchildの終了が完了したことを返します。
func (m *DeliveryMultiLogger) ShutdownComplete() bool {
	if m == nil {
		return true
	}
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return m.closeState == loggercommon.LoggerCloseStateCompleted
}
