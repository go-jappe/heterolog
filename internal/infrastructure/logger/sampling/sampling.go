package sampling

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/pkg/logger"
	"hash/fnv"
	"log"
	"math/rand"
)

// ShouldSampleByRequestId deterministically selects a request. Invalid rates
// are rejected by constructors; direct callers also receive false safely.
func ShouldSampleByRequestId(key string, rate int) bool {
	if rate <= 0 || uint64(rate) > MaxRequestIDSamplingRate {
		log.Printf("[WARN]no sampling due to invalid rate. rate:%v\n", rate)
		return false
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()%uint32(rate) == 0
}

type SamplingRateConfig interface {
	SamplingErrorRate() int
	SamplingWarnRate() int
	SamplingInfoRate() int
	SamplingDebugRate() int
	IsSamplingEnabled() bool
	LoggingByRequestIDRate() int
	EnableLoggingByRequestID() bool
}

func LevelBasedSampling(level logger.LogLevel, config SamplingRateConfig) bool {

	switch level {
	case logger.LogLevelError:
		return rand.Intn(config.SamplingErrorRate()) == 0
	case logger.LogLevelWarn:
		return rand.Intn(config.SamplingWarnRate()) == 0
	case logger.LogLevelInfo:
		return rand.Intn(config.SamplingInfoRate()) == 0
	case logger.LogLevelDebug:
		return rand.Intn(config.SamplingDebugRate()) == 0
	default:
		return false
	}
}

func NeedLog(entry *logging.LogEntry, config SamplingRateConfig) bool {
	// request_idでサンプリングする設定でレートが1ならば、それはサンプリングではない
	if config.EnableLoggingByRequestID() && config.LoggingByRequestIDRate() == 1 && entry.RequestID != "" {
		return true
	}

	// 該当レベルでサンプリングレートが1ならば、それはサンプリングではない
	switch entry.Level {
	case logger.LogLevelError:
		return config.SamplingErrorRate() == 1
	case logger.LogLevelWarn:
		return config.SamplingWarnRate() == 1
	case logger.LogLevelInfo:
		return config.SamplingInfoRate() == 1
	case logger.LogLevelDebug:
		return config.SamplingDebugRate() == 1
	default:
		return false
	}
}

func ShouldSampling(entry *logging.LogEntry, config SamplingRateConfig) bool {
	// リクエストID追跡　サンプリング
	// リクエストidが未設定(stringの初期値)ならば、スキップ（request_id追跡サンプリングは行わない）
	if entry.RequestID != "" && config.EnableLoggingByRequestID() {
		return ShouldSampleByRequestId(entry.RequestID, config.LoggingByRequestIDRate())
	}

	// リクエストID追跡型でなければログレベルでサンプリング
	return LevelBasedSampling(entry.Level, config)
}
