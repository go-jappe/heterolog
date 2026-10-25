package duration

import (
	"fmt"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"time"
)

type LogFunc func(level pkglogger.LogLevel, elapsed time.Duration, meta any)

// LogLevelForElapsedは経過時間が閾値を超えた場合にError、Warnの順で判定し、
// どちらも超えていなければDebugを返します。閾値と同じ時間は超過に含めません。
func LogLevelForElapsed(elapsed, errorThreshold, warnThreshold time.Duration) pkglogger.LogLevel {
	switch {
	case elapsed > errorThreshold:
		return pkglogger.LogLevelError
	case elapsed > warnThreshold:
		return pkglogger.LogLevelWarn
	default:
		return pkglogger.LogLevelDebug
	}
}

func MeasureAndLogDuration[T any](
	task func() (T, error),
	logFunc LogFunc,
	context string,
	meta any,
	errorThreshold time.Duration,
	warnThreshold time.Duration,
) (T, error) {
	var zero T
	start := time.Now()
	res, err := task()
	elapsed := time.Since(start)

	if err != nil {
		return zero, fmt.Errorf("failed to %s; delay %v; %w", context, elapsed, err)
	}

	logFunc(LogLevelForElapsed(elapsed, errorThreshold, warnThreshold), elapsed, meta)
	return res, nil
}

// openseachなどnosqldb bulk送信用
func MeasureAndIndexDocumentsDuration[T any](
	task func() (T, error),
	logFunc LogFunc,
	context string,
	meta any,
	errorThreshold time.Duration,
	warnThreshold time.Duration,
) (T, error) {
	start := time.Now()
	resp, err := task()
	elapsed := time.Since(start)

	if err != nil {
		return resp, fmt.Errorf("failed to %s; delay %v; %w", context, elapsed, err)
	}

	logFunc(LogLevelForElapsed(elapsed, errorThreshold, warnThreshold), elapsed, meta)
	return resp, nil
}
