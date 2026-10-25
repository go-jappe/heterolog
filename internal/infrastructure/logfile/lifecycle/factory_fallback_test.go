package lifecycle

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestNewLogFileLifecyclesAllowsUnknownRotationForDailyFallback(t *testing.T) {
	cfg := pkgfsstore.LogFileLifecycleRunnerConfig{
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				Target:         "daily-fallback-factory",
				LogPath:        resolvedTempDirForTest(t),
				LogDir:         "logs",
				FileNameFormat: pkgfsstore.FileNameFormat,
				FileDateFormat: pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				UsageCacheTTL:      time.Hour,
				EnableRotate:       true,
				RotationType:       pkgfsstore.FileRotationType("hourly"),
				MaxDirSize:         1024,
				EnableFileCompress: false,
				CompressFormat:     pkgfsstore.CompressGzip,
			},
		},
	}

	lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})

	require.NoError(t, err)
	require.Len(t, lifecycles, 1)
}
