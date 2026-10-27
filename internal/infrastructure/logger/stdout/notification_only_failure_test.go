package stdout

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

	"github.com/stretchr/testify/require"
)

// 内部ログの通知先として作るstdout handler（通知専用instance）は通知先を持たないため、自身の
// 書き込みの失敗を通知せず、失敗ごとに一行、標準loggerへ残します（docs/reference.ja.mdの
// 「内部ログの通知」）。process全体で一度だけの診断ではありません。
// 標準出力・標準エラー・標準loggerを置き換えるため、t.Parallelを使いません。
func TestNotificationOnlyStdoutHandlersWriteEachFailureToStandardLog(t *testing.T) {
	const failures = 3
	cfg := NewConfigAdapter(pkglogger.StdoutLoggerConfig{
		TimeConfig:    pkglogger.TimeConfig{TimeZone: "UTC"},
		LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339},
	})
	target, err := NewNotificationTarget(cfg, nil)
	require.NoError(t, err)
	errorHandler := target.NamedErrorHandler().Handler
	for _, sink := range []struct {
		name    string
		failing string
		notify  logging.LoggerHandlerFunc
		level   pkglogger.LogLevel
	}{
		// INFOの内部ログは標準出力へ、ERRORの内部ログは標準エラーへ書き込みます。
		{name: "info_to_stdout", failing: "stdout", notify: target.NamedInfoHandler().Handler, level: pkglogger.LogLevelInfo},
		{name: "error_to_stderr", failing: "stderr", notify: func(ctx context.Context, entry *logging.LogEntry, tracer pkgobserver.Tracer, meter pkgobserver.Meter) {
			errorHandler(ctx, nil, entry, tracer, meter)
		}, level: pkglogger.LogLevelError},
	} {
		t.Run(sink.name, func(t *testing.T) {
			if sink.failing == "stdout" {
				replaceStdoutTestStreamWithClosedFile(t, &os.Stdout)
			} else {
				replaceStdoutTestStreamWithClosedFile(t, &os.Stderr)
			}
			var standardLog bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&standardLog)
			t.Cleanup(func() { log.SetOutput(previous) })

			for index := range failures {
				sink.notify(context.Background(), &logging.LogEntry{
					Level: sink.level, Message: fmt.Sprintf("internal log %d", index), Internal: true,
				}, observer.NopTracer{}, observer.NopMeter{})
			}

			// 標準loggerの行は運用者が読む出力なので、失敗したstreamを示す文言で数えます。
			want := fmt.Sprintf(MsgFailedWriteNotification, "failed to write to "+sink.failing+".")
			var matched int
			for _, line := range strings.Split(strings.TrimSpace(standardLog.String()), "\n") {
				if strings.Contains(line, want) {
					matched++
				}
			}
			require.Equal(t, failures, matched, "one standard log line per failed notification:\n%s", standardLog.String())
		})
	}
}
