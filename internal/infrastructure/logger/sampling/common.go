package sampling

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/pkg/logger"
)

// ProcessLogSamplingは、すべてのloggerに共通のsamplingの判定を行います。logを
// 処理するかどうかと、samplingで選んだかどうかを返します。
func ProcessLogSampling(meta *logging.LogEntry, config SamplingRateConfig) (bool, bool) {
	return processLogSampling(meta, snapshotOf(config))
}

// processLogSamplingは、一度に読み取った設定configでProcessLogSamplingの判定を行います。
func processLogSampling(meta *logging.LogEntry, config logger.SamplingConfig) (bool, bool) {
	// samplingが無効な場合は、すべてのlogをsamplingせずに処理します。
	if !config.IsSamplingEnabled {
		return true, false
	}

	// 必ず記録するlevelのlogは、samplingせずに処理します。
	if needLog(meta, config) {
		return true, false
	}

	// samplingで選ぶlogかを確かめます。
	if !shouldSampling(meta, config) {
		// samplingの条件を満たさないlogは処理しません。
		return false, false
	}

	// samplingで選んだ印を付けて処理します。
	return true, true
}

// snapshotOfは、判定に使う設定をconfigから一度に読み取ります。稼働中に変更できる
// providerでも、一回の判定は一つの設定の値だけで行います。
func snapshotOf(config SamplingRateConfig) logger.SamplingConfig {
	if snapshot, ok := config.(samplingSnapshot); ok {
		return snapshot.SamplingConfig()
	}
	return logger.SamplingConfig{
		IsSamplingEnabled:        config.IsSamplingEnabled(),
		EnableLoggingByRequestID: config.EnableLoggingByRequestID(),
		LoggingByRequestIDRate:   config.LoggingByRequestIDRate(),
		SamplingDebugRate:        config.SamplingDebugRate(),
		SamplingInfoRate:         config.SamplingInfoRate(),
		SamplingWarnRate:         config.SamplingWarnRate(),
		SamplingErrorRate:        config.SamplingErrorRate(),
	}
}

// ApplySamplingLogicは、共通のsamplingの判定を行い、LogEntryを更新します。logを
// 処理する場合はtrue、処理しない場合はfalseを返します。
func ApplySamplingLogic(meta *logging.LogEntry, config SamplingRateConfig) bool {
	shouldProcess, wasSampled := ProcessLogSampling(meta, config)

	if shouldProcess {
		meta.Sampled = wasSampled
	}

	return shouldProcess
}
