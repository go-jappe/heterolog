package otel

import (
	"fmt"
	"strings"
)

// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/logs/data-model.md#field-severitynumber
// otelの上のドキュメントに準拠

type LogLevel int

const (
	LogLevelTrace = 1
	LogLevelDebug = 5
	LogLevelInfo  = 9
	LogLevelWarn  = 13
	LogLevelError = 17
	LogLevelFatal = 21
)

// LogLevel から文字列へのマッピング
var LogLevelToString = map[LogLevel]string{
	LogLevelTrace: "TRACE",
	LogLevelDebug: "DEBUG",
	LogLevelInfo:  "INFO",
	LogLevelWarn:  "WARN",
	LogLevelError: "ERROR",
	LogLevelFatal: "FATAL",
}

// 文字列から LogLevel へのマッピング
var LogLevelMaps = map[string]int{
	"TRACE": LogLevelTrace,
	"DEBUG": LogLevelDebug,
	"INFO":  LogLevelInfo,
	"WARN":  LogLevelWarn,
	"ERROR": LogLevelError,
	"FATAL": LogLevelFatal,
}

func ParseLogLevel(level string) (int, error) {
	if lvl, ok := LogLevelMaps[strings.ToUpper(level)]; ok {
		return lvl, nil
	}
	return 0, fmt.Errorf("invalid log level: %s", level)
}

func GetSeverityText(n int) string {
	switch {
	case n >= 1 && n <= 4:
		return "TRACE"
	case n >= 5 && n <= 8:
		return "DEBUG"
	case n >= 9 && n <= 12:
		return "INFO"
	case n >= 13 && n <= 16:
		return "WARN"
	case n >= 17 && n <= 20:
		return "ERROR"
	case n >= 21 && n <= 24:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}
