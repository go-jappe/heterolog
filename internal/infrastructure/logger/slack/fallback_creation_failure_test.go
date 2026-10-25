package slack

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// 内部ログの通知先のfallback loggerを作れなかった場合も、通知を黙って捨てません。作成は
// 再試行せず、作れなかった通知先への通知ごとに標準loggerへ一行残します。設定は通常
// top-level loggerの構築時に検証されるため、ここではその検証を経ずに通知先のhandlerを作り、
// 通知の初回の作成失敗を起こします（MaxLogsPerNotificationが0のfallback loggerは作れません）。
func TestSlackFallbackHandlerLogsEveryNotificationWhenLoggerCannotBeCreated(t *testing.T) {
	standardLog := captureSlackStandardLog(t)
	named := NewNamedErrorFallbackHandler(NewConfigAdapter(pkglogger.SlackLoggerConfig{
		Token:                  "xoxb-test-token",
		Channel:                "test-channel",
		Timeout:                time.Second,
		MaxLogsPerNotification: 0,
	}))
	t.Cleanup(func() {
		if err := named.Closer.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	const notifications = 3
	for i := range notifications {
		named.Handler(context.Background(), errors.New("source logger failure"), &logging.LogEntry{
			Level:   pkglogger.LogLevelError,
			Message: fmt.Sprintf("notification %d", i),
		}, observer.NopTracer{}, observer.NopMeter{})
	}

	// 標準loggerへの行には型がないため、作成に失敗した通知先の行を文言で数えます。
	failureLine := strings.TrimSuffix(MsgFailedCreateFallbackLogger, "%v")
	if got := standardLog.count(failureLine); got != notifications {
		t.Fatalf("fallback creation failure lines = %d, want one per notification (%d):\n%s", got, notifications, standardLog)
	}
}
