package bootstrap_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/bootstrap"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
)

// 内部ログの通知先だけに使うstdout（通常出力ではないhandler target）の表示設定が不正なら、
// New・NewAsync・NewDeliveryは、通知の初回に書き込み先を作れず通知を捨てるのではなく、構築を
// 設定error（ErrInvalidNotificationTarget）で終えます。どのloggerのどの通知先かは
// NotificationTargetErrorのSource・Backendで特定できます。
func TestConstructorsRejectInvalidHandlerOnlyStdoutNotificationSettings(t *testing.T) {
	notifyStdout := pkgloggerhandler.HandlerConfig{OnErrorConfig: []string{pkglogger.LogOutputStdout.String()}}
	for _, setting := range []struct {
		name  string
		apply func(*pkglogger.TimeConfig, *pkglogger.LogBodyConfig)
	}{
		{name: "unknown_timezone", apply: func(time *pkglogger.TimeConfig, _ *pkglogger.LogBodyConfig) {
			time.IsLocalTimeEnabled = true
			time.TimeZone = "No/Such_Zone"
		}},
		{name: "empty_date_format", apply: func(_ *pkglogger.TimeConfig, body *pkglogger.LogBodyConfig) {
			body.LogLineDateFormat = ""
		}},
	} {
		t.Run(setting.name, func(t *testing.T) {
			t.Run("logger", func(t *testing.T) {
				cfg := directoryWhitespaceConfig(t.TempDir(), "notification-target", "logs")
				cfg.File.HandlerConfig = notifyStdout
				cfg.Stdout = bootstrap.DefaultConfig().Stdout
				setting.apply(&cfg.Stdout.TimeConfig, &cfg.Stdout.LogBodyConfig)
				requireNotificationTargetError(t, closeConstructed(bootstrap.New(context.Background(), cfg)), "logger")
			})
			t.Run("async logger", func(t *testing.T) {
				cfg := bootstrap.DefaultAsyncConfig()
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				cfg.File = directoryWhitespaceConfig(t.TempDir(), "notification-target", "logs").File
				cfg.File.HandlerConfig = notifyStdout
				setting.apply(&cfg.Stdout.TimeConfig, &cfg.Stdout.LogBodyConfig)
				requireNotificationTargetError(t, closeConstructed(bootstrap.NewAsync(context.Background(), cfg)), "async logger")
			})
			t.Run("delivery logger", func(t *testing.T) {
				fileCfg := directoryWhitespaceConfig(t.TempDir(), "notification-target", "logs").File
				cfg := bootstrap.DefaultDeliveryConfig()
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				cfg.File = &pkglogger.DeliveryFileLoggerConfig{
					FsstoreConfig: pkgfsstore.DeliveryFileWriteConfig{
						Layout:   fileCfg.FsstoreConfig.Layout,
						Rotation: pkgfsstore.FileRotationConfig{MaxFileSize: fileCfg.FsstoreConfig.Lifecycle.MaxFileSize},
						Duration: fileCfg.FsstoreConfig.Duration,
						Retry:    fileCfg.FsstoreConfig.Retry,
					},
					TimeConfig:    fileCfg.TimeConfig,
					LogBodyConfig: fileCfg.LogBodyConfig,
					HandlerConfig: notifyStdout,
				}
				setting.apply(&cfg.Stdout.TimeConfig, &cfg.Stdout.LogBodyConfig)
				requireNotificationTargetError(t, closeConstructed(bootstrap.NewDelivery(context.Background(), cfg)), "delivery logger")
			})
		})
	}
}

// requireNotificationTargetErrorは、errがsourceのloggerでstdoutの通知先を作れない構築時の
// 設定errorであることを確かめます。
func requireNotificationTargetError(t *testing.T, err error, source string) {
	t.Helper()
	if !errors.Is(err, pkglogger.ErrInvalidNotificationTarget) {
		t.Fatalf("constructor error = %v, want ErrInvalidNotificationTarget", err)
	}
	var targetErr *pkglogger.NotificationTargetError
	if !errors.As(err, &targetErr) || targetErr.Source != source || targetErr.Backend != pkglogger.LogOutputStdout {
		t.Fatalf("constructor error = %v (%+v), want the stdout notification target of the %s", err, targetErr, source)
	}
}
