package logger

import (
	"errors"
	"testing"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestNewDeliveryFileLoggerSetRejectsInlineLifecycleMode(t *testing.T) {
	_, err := newDeliveryFileLoggerSet(
		&pkglogger.DeliveryFileLoggerConfig{
			FsstoreConfig: pkgfsstore.DeliveryFileWriteConfig{
				Layout: pkgfsstore.FsstoreConfig{
					LogPath: t.TempDir(), Target: "app", LogDir: "logs",
				},
				// lifecycle mode以外は有効にし、inline modeだけで拒否されることを確認します。
				Rotation: pkgfsstore.FileRotationConfig{
					RotationType: pkgfsstore.FileRotationDaily,
					MaxFileSize:  infrafsstore.MinAllowedFileSizeBytes,
				},
			},
			LifecycleMode: pkgfsstore.FileLifecycleModeInline,
		},
		nil,
		nil,
	)
	if !errors.Is(err, pkgfsstore.ErrUnsupportedDeliveryFileLifecycleMode) {
		t.Fatalf(
			"newDeliveryFileLoggerSet() error = %v, want %v",
			err,
			pkgfsstore.ErrUnsupportedDeliveryFileLifecycleMode,
		)
	}
}
