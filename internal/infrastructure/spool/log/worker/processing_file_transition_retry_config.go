package worker

import "time"

// ProcessingFileTransitionRetryConfigProviderは、processing fileの状態遷移retryが
// 参照する待機時間と保持件数の設定です。
type ProcessingFileTransitionRetryConfigProvider interface {
	RetryDelay() time.Duration
	MaxEntries() int
}

// ProcessingFileTransitionRetryConfigAdapterは設定を値として保持し、補完や検証を行いません。
type ProcessingFileTransitionRetryConfigAdapter struct {
	config ProcessingFileTransitionRetryConfig
}

var _ ProcessingFileTransitionRetryConfigProvider = ProcessingFileTransitionRetryConfigAdapter{}

func NewProcessingFileTransitionRetryConfigAdapter(
	cfg ProcessingFileTransitionRetryConfig,
) ProcessingFileTransitionRetryConfigAdapter {
	return ProcessingFileTransitionRetryConfigAdapter{config: cfg}
}

func (a ProcessingFileTransitionRetryConfigAdapter) RetryDelay() time.Duration {
	return a.config.RetryDelay
}

func (a ProcessingFileTransitionRetryConfigAdapter) MaxEntries() int {
	return a.config.MaxEntries
}
