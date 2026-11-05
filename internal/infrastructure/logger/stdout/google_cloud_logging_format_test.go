package stdout

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// 環境変数からの設定はOpenTelemetry形式が既定で有効なため、Cloud Logging形式を選ぶだけで
// 切り替わることを確かめます。resourceと時刻の設定も使わないため、構築は失敗しません。
func TestNotificationTargetPrefersGoogleCloudLoggingFormat(t *testing.T) {
	t.Parallel()

	target, err := NewNotificationTarget(NewConfigAdapter(pkglogger.StdoutLoggerConfig{
		TimeConfig:                  pkglogger.TimeConfig{IsLocalTimeEnabled: true, TimeZone: "Invalid/Zone"},
		UseOpenTelemetryFormat:      true,
		UseGoogleCloudLoggingFormat: true,
	}), nil)
	require.NoError(t, err)
	require.IsType(t, formatter.GoogleCloudLoggingFormatter{}, target.formatter)
}

func TestDeliveryFallbackConfigAdapterKeepsGoogleCloudLoggingFormat(t *testing.T) {
	t.Parallel()

	adapter := NewDeliveryFallbackConfigAdapter(pkglogger.DeliveryStdoutLoggerConfig{
		UseGoogleCloudLoggingFormat: true,
	})
	require.True(t, adapter.UseGoogleCloudLoggingFormat())
}
