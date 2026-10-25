package file

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
)

// Delivery用のfile loggerも通常のfile loggerと同じく、1ファイルの最大サイズの下限を
// rotationの有無にかかわらず、保存先を作成する前に検証します。
func TestFileLoggerConstructorsShareMinimumMaxFileSize(t *testing.T) {
	constructors := []struct {
		name      string
		construct func(t *testing.T, root string, maxFileSize uint64, rotate bool) error
	}{
		{name: "sync set", construct: func(t *testing.T, root string, maxFileSize uint64, rotate bool) error {
			cfg := newTestFileLoggerConfig(root, "logs", "max-file-size")
			cfg.FsstoreConfig.Lifecycle.EnableRotate = rotate
			cfg.FsstoreConfig.Lifecycle.MaxFileSize = maxFileSize
			set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
			if set != nil {
				closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
			}
			return err
		}},
		{name: "sync default", construct: func(t *testing.T, root string, maxFileSize uint64, rotate bool) error {
			cfg := newTestFileLoggerConfig(root, "logs", "max-file-size")
			cfg.FsstoreConfig.Lifecycle.EnableRotate = rotate
			cfg.FsstoreConfig.Lifecycle.MaxFileSize = maxFileSize
			logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
			if logger != nil {
				closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
			}
			return err
		}},
		{name: "delivery set", construct: func(t *testing.T, root string, maxFileSize uint64, rotate bool) error {
			cfg := newTestDeliveryFileLoggerConfig(root, "logs", "max-file-size")
			cfg.FsstoreConfig.Rotation.EnableRotate = rotate
			cfg.FsstoreConfig.Rotation.MaxFileSize = maxFileSize
			set, err := NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
			if set != nil {
				closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
			}
			return err
		}},
		{name: "delivery default", construct: func(t *testing.T, root string, maxFileSize uint64, rotate bool) error {
			return constructDeliveryFileLoggerForSizeTest(t, NewDeliveryFileLogger, root, maxFileSize, rotate)
		}},
		{name: "delivery error", construct: func(t *testing.T, root string, maxFileSize uint64, rotate bool) error {
			return constructDeliveryFileLoggerForSizeTest(t, NewDeliveryErrorFileLogger, root, maxFileSize, rotate)
		}},
		{name: "delivery debug", construct: func(t *testing.T, root string, maxFileSize uint64, rotate bool) error {
			return constructDeliveryFileLoggerForSizeTest(t, NewDeliveryDebugFileLogger, root, maxFileSize, rotate)
		}},
	}

	for _, rotate := range []bool{false, true} {
		for _, constructor := range constructors {
			t.Run(fmt.Sprintf("%s/rotate=%t", constructor.name, rotate), func(t *testing.T) {
				// 下限ちょうどとの違いはMaxFileSizeだけです。下限ちょうどを受理し、1byte小さい値だけを
				// 拒否することで、拒否の原因がファイルサイズの下限であることを確認します。
				require.NoError(t, constructor.construct(t, t.TempDir(), fsstore.MinAllowedFileSizeBytes, rotate))

				root := t.TempDir()
				require.ErrorIs(t, constructor.construct(t, root, fsstore.MinAllowedFileSizeBytes-1, rotate), fsstore.ErrInvalidMaxFileSize)
				entries, err := os.ReadDir(root)
				require.NoError(t, err)
				require.Empty(t, entries, "invalid MaxFileSize must not create log directories or files")
			})
		}
	}
}

func constructDeliveryFileLoggerForSizeTest(
	t *testing.T,
	newLogger func(context.Context, DeliveryFileLoggerConfigProvider, observer.Tracer, observer.Meter) (*DeliveryFileLogger, error),
	root string,
	maxFileSize uint64,
	rotate bool,
) error {
	t.Helper()
	cfg := newTestDeliveryFileLoggerConfig(root, "logs", "max-file-size")
	cfg.FsstoreConfig.Rotation.EnableRotate = rotate
	cfg.FsstoreConfig.Rotation.MaxFileSize = maxFileSize
	logger, err := newLogger(context.Background(), NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
	if logger != nil {
		closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
	}
	return err
}
