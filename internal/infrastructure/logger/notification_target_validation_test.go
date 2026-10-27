package logger

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

// notificationTargetUnreachableEndpointは、閉じたlocal portです。通知先の構築時の検証は
// requestを送らないため、どの宛先へも接続しません。
const notificationTargetUnreachableEndpoint = "http://127.0.0.1:1"

// invalidNotificationTimeSettingsは、通知先の書き込み先を作れない表示設定と、その原因です。
var invalidNotificationTimeSettings = []struct {
	name  string
	apply func(*pkglogger.TimeConfig, *pkglogger.LogBodyConfig)
	cause error
}{
	{name: "unknown_timezone", apply: func(time *pkglogger.TimeConfig, _ *pkglogger.LogBodyConfig) {
		time.IsLocalTimeEnabled = true
		time.TimeZone = "No/Such_Zone"
	}, cause: logging.ErrInvalidTimeZone},
	{name: "empty_date_format", apply: func(_ *pkglogger.TimeConfig, body *pkglogger.LogBodyConfig) {
		body.LogLineDateFormat = ""
	}, cause: timestamp.ErrLayoutRequired},
}

// requireNotificationTargetErrorは、errがsourceのloggerの通知先backendを作れない構築時の設定errorで、
// 原因がcauseであることを確かめます。
func requireNotificationTargetError(t *testing.T, err error, source string, backend pkglogger.LogOutputDestination, cause error) {
	t.Helper()
	require.ErrorIs(t, err, pkglogger.ErrInvalidNotificationTarget)
	require.ErrorIs(t, err, cause)
	var targetErr *pkglogger.NotificationTargetError
	require.ErrorAs(t, err, &targetErr)
	require.Equal(t, source, targetErr.Source)
	require.Equal(t, backend, targetErr.Backend)
}

// requireNoStorageCreatedは、構築に失敗したloggerが保存先へ何も作っていないことを確かめます。
func requireNoStorageCreated(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Empty(t, entries, "configuration errors must be reported before any storage is created")
}

// closeConstructedForTestは、構築できたloggerを停止の検出付きで閉じます。
func closeConstructedForTest(t *testing.T, closer io.Closer) {
	t.Helper()
	require.NoError(t, boundedwait.Close(t, "logger Close", closer.Close, boundedwait.ForceCloserOf(closer)))
}

// 内部ログの通知先だけに使うstdout（通常出力ではないhandler target）の表示設定が不正なら、
// 通知の初回に書き込み先を作れずに通知を捨てるのではなく、loggerの構築を設定errorで終えます。
// 検証は保存先を作る前に行います。
func TestConstructorsRejectInvalidHandlerOnlyStdoutNotificationTarget(t *testing.T) {
	for _, setting := range invalidNotificationTimeSettings {
		t.Run(setting.name, func(t *testing.T) {
			t.Run("sync", func(t *testing.T) {
				cfg, _, _ := newHandlerOnlyLoggerConfig(t, pkglogger.LogOutputStdout)
				setting.apply(&cfg.Stdout.TimeConfig, &cfg.Stdout.LogBodyConfig)
				_, err := NewLogger(context.Background(), cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, observer.NopTracer{}, observer.NopMeter{})
				requireNotificationTargetError(t, err, pkglogger.LoggerContractSync.DisplayName(), pkglogger.LogOutputStdout, setting.cause)
				requireNoStorageCreated(t, cfg.File.FsstoreConfig.Layout.LogPath)
			})
			t.Run("async", func(t *testing.T) {
				base, _, _ := newHandlerOnlyLoggerConfig(t, pkglogger.LogOutputStdout)
				setting.apply(&base.Stdout.TimeConfig, &base.Stdout.LogBodyConfig)
				cfg := pkglogger.AsyncBestEffortMultiLoggerConfig{
					LogOutputDestinations: base.LogOutputDestinations,
					Timeout:               base.Timeout,
					MaxParallel:           1,
					MaxLogEntryBytes:      base.MaxLogEntryBytes,
					File:                  base.File,
					Stdout:                base.Stdout,
				}
				_, err := NewAsyncLogger(context.Background(), cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, observer.NopTracer{}, observer.NopMeter{})
				requireNotificationTargetError(t, err, pkglogger.LoggerContractAsyncBestEffort.DisplayName(), pkglogger.LogOutputStdout, setting.cause)
				requireNoStorageCreated(t, base.File.FsstoreConfig.Layout.LogPath)
			})
			t.Run("delivery", func(t *testing.T) {
				cfg, _, _ := newHandlerOnlyDeliveryConfig(t, pkglogger.LogOutputStdout)
				setting.apply(&cfg.Stdout.TimeConfig, &cfg.Stdout.LogBodyConfig)
				_, err := NewDeliveryLogger(context.Background(), cfg, pkgcloudwatchlogs.StreamRoutingNone, observer.NopTracer{}, observer.NopMeter{})
				requireNotificationTargetError(t, err, pkglogger.LoggerContractDelivery.DisplayName(), pkglogger.LogOutputStdout, setting.cause)
				requireNoStorageCreated(t, cfg.File.FsstoreConfig.Layout.LogPath)
			})
		})
	}
}

// 通知先として選ばれていないstdoutの設定は通知に使わないため、通知先としては検証しません
// （通常出力やhandler targetとして使わないbase設定を検証しない規則と同じです）。
func TestUnselectedStdoutConfigIsNotValidatedAsNotificationTarget(t *testing.T) {
	cfg, _, _ := newHandlerOnlyLoggerConfig(t, pkglogger.LogOutputStdout)
	cfg.File.HandlerConfig.OnErrorConfig = nil
	invalidNotificationTimeSettings[0].apply(&cfg.Stdout.TimeConfig, &cfg.Stdout.LogBodyConfig)
	logger, err := NewLogger(context.Background(), cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, observer.NopTracer{}, observer.NopMeter{})
	require.NoError(t, err)
	closeConstructedForTest(t, logger.(io.Closer))
}

// network backendを内部ログの通知先だけに使う場合も、通知専用のfallback loggerを通知の初回と
// 同じ手順で作れることを構築時に確かめ、作れない設定は設定errorにします。検証は送信を行いません。
func TestConstructorsRejectInvalidHandlerOnlyNetworkNotificationTarget(t *testing.T) {
	t.Run("sync/loki", func(t *testing.T) {
		newConfig := func(t *testing.T) pkglogger.LoggerConfig {
			cfg, _, _ := newHandlerOnlyLoggerConfig(t, pkglogger.LogOutputLoki)
			cfg.LokiClient.Endpoint = notificationTargetUnreachableEndpoint
			cfg.Loki.LokiLoggerBaseConfig.LogBodyConfig.LogLineDateFormat = time.RFC3339Nano
			return cfg
		}
		// 対照: 通知先を作れる設定は構築できます。
		logger, err := NewLogger(context.Background(), newConfig(t), pkgcloudwatchlogs.StreamRoutingNone, nil, nil, observer.NopTracer{}, observer.NopMeter{})
		require.NoError(t, err)
		closeConstructedForTest(t, logger.(io.Closer))

		cfg := newConfig(t)
		invalidNotificationTimeSettings[0].apply(&cfg.Loki.LokiLoggerBaseConfig.TimeConfig, &cfg.Loki.LokiLoggerBaseConfig.LogBodyConfig)
		_, err = NewLogger(context.Background(), cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, observer.NopTracer{}, observer.NopMeter{})
		requireNotificationTargetError(t, err, pkglogger.LoggerContractSync.DisplayName(), pkglogger.LogOutputLoki, logging.ErrInvalidTimeZone)
	})
	t.Run("delivery/opensearch", func(t *testing.T) {
		newConfig := func(t *testing.T) pkglogger.DeliveryLoggerConfig {
			cfg, _, _ := newHandlerOnlyDeliveryConfig(t, pkglogger.LogOutputOpenSearch)
			cfg.OpenSearchClient.Endpoint = notificationTargetUnreachableEndpoint
			cfg.OpenSearch.DeliveryOpenSearchLoggerBaseConfig.LogBodyConfig.LogLineDateFormat = time.RFC3339Nano
			return cfg
		}
		logger, err := NewDeliveryLogger(context.Background(), newConfig(t), pkgcloudwatchlogs.StreamRoutingNone, observer.NopTracer{}, observer.NopMeter{})
		require.NoError(t, err)
		closeConstructedForTest(t, logger)

		cfg := newConfig(t)
		base := &cfg.OpenSearch.DeliveryOpenSearchLoggerBaseConfig
		invalidNotificationTimeSettings[0].apply(&base.TimeConfig, &base.LogBodyConfig)
		_, err = NewDeliveryLogger(context.Background(), cfg, pkgcloudwatchlogs.StreamRoutingNone, observer.NopTracer{}, observer.NopMeter{})
		requireNotificationTargetError(t, err, pkglogger.LoggerContractDelivery.DisplayName(), pkglogger.LogOutputOpenSearch, logging.ErrInvalidTimeZone)
	})
	t.Run("errors_are_joined_per_target", func(t *testing.T) {
		// 複数の通知先が不正なら、どの通知先も一度の構築errorで報告します。
		cfg, _, _ := newHandlerOnlyLoggerConfig(t, pkglogger.LogOutputLoki)
		cfg.LokiClient.Endpoint = notificationTargetUnreachableEndpoint
		invalidNotificationTimeSettings[0].apply(&cfg.Loki.LokiLoggerBaseConfig.TimeConfig, &cfg.Loki.LokiLoggerBaseConfig.LogBodyConfig)
		addLoggerHandlerCandidate(t, &cfg, pkglogger.LogOutputSlack)
		cfg.Stdout.HandlerConfig.OnErrorConfig = append(cfg.Stdout.HandlerConfig.OnErrorConfig, pkglogger.LogOutputSlack.String())
		invalidNotificationTimeSettings[1].apply(&cfg.Slack.TimeConfig, &cfg.Slack.LogBodyConfig)
		_, err := NewLogger(context.Background(), cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, observer.NopTracer{}, observer.NopMeter{})
		causes := map[pkglogger.LogOutputDestination]error{}
		for _, targetErr := range errortree.All[*pkglogger.NotificationTargetError](err) {
			causes[targetErr.Backend] = targetErr.Err
		}
		require.Len(t, causes, 2, "%v", err)
		require.ErrorIs(t, causes[pkglogger.LogOutputLoki], logging.ErrInvalidTimeZone)
		require.ErrorIs(t, causes[pkglogger.LogOutputSlack], timestamp.ErrLayoutRequired)
	})
}
