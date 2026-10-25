package slack

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
)

// ValidateFallbackLoggerConfigは、内部ログの通知先として使うfallback loggerを、通知の初回に
// 作るときと同じ手順（newHandleSlackLogger）で作って直ちに閉じ、その設定から通知先を作れることを
// loggerの構築時に確かめます。通知の初回に作れずに通知を黙って捨てることがないよう、
// top-level loggerの構築がNewNamed*FallbackHandlerと同じ設定で呼び出します。送信は行いません。
func ValidateFallbackLoggerConfig(adapter SlackLoggerConfigProvider) error {
	fallback, err := newHandleSlackLogger(context.Background(), adapter, observer.NopTracer{}, observer.NopMeter{})
	if err != nil {
		return err
	}
	return fallback.Close()
}
