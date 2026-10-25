package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// clickHouseStandardLogは、試験の間だけ標準loggerの出力を記録します。標準loggerはprocess全体で
// 共有されるため、t.Parallelの試験では使いません。
type clickHouseStandardLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func captureClickHouseStandardLog(t *testing.T) *clickHouseStandardLog {
	t.Helper()
	capture := &clickHouseStandardLog{}
	previous := log.Writer()
	log.SetOutput(capture)
	t.Cleanup(func() { log.SetOutput(previous) })
	return capture
}

func (l *clickHouseStandardLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

// countは、substrが出力に現れる回数を返します。
func (l *clickHouseStandardLog) count(substr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Count(l.buf.String(), substr)
}

func (l *clickHouseStandardLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// 内部ログの通知先のfallback loggerを作れなかった場合も、通知を黙って捨てません。作成は
// 再試行せず、作れなかった通知先への通知ごとに標準loggerへ一行残します。設定は通常
// top-level loggerの構築時に検証されるため、ここではその検証を経ずに通知先のhandlerを作り、
// 通知の初回の作成失敗を起こします（MaxEntriesが0のfallback loggerは作れません）。
func TestClickHouseFallbackHandlerLogsEveryNotificationWhenLoggerCannotBeCreated(t *testing.T) {
	standardLog := captureClickHouseStandardLog(t)
	named := NewNamedErrorFallbackHandler(
		NewClickHouseLoggerConfigAdapter(pkglogger.ClickHouseLoggerBaseConfig{
			FlushInterval:  time.Hour,
			MaxEntries:     0,
			SamplingConfig: validClickHouseMaxEntriesTestSamplingConfig(),
		}),
		nil, nil, nil, nil,
	)
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
	if got := standardLog.count("failed to create fallback ClickHouse logger"); got != notifications {
		t.Fatalf("fallback creation failure lines = %d, want one per notification (%d):\n%s", got, notifications, standardLog)
	}
}
