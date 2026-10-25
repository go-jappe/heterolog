package lifecycle

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestLifecycleFactoriesRejectEmptyLogDirectoryBeforeCreatingFiles(t *testing.T) {
	for _, logDir := range []string{"", " \t\r\n"} {
		for _, factory := range []string{
			"log lifecycles",
			"delivery lifecycles",
		} {
			t.Run(fmt.Sprintf("%s/logDir=%q", factory, logDir), func(t *testing.T) {
				cfg := configuredCheckpointFactoryTestConfig(t)
				cfg.RunnerConfig.Interval = time.Minute
				cfg.FsstoreConfig.Layout.LogDir = logDir
				err := invokeLogFileNameValidationFactory(factory, cfg)
				require.ErrorIs(t, err, pkgfsstore.ErrEmptyLogDirectory)
				entries, readErr := os.ReadDir(cfg.FsstoreConfig.Layout.LogPath)
				require.NoError(t, readErr)
				require.Empty(t, entries, "invalid LogDir must not create log or checkpoint directories or files")
			})
		}
	}
}

func TestFileSeriesManagerDoesNotUseConfigLogDirectoryWithExplicitFileName(t *testing.T) {
	for _, logDir := range []string{"", " \t\r\n", ".", "logs"} {
		t.Run(fmt.Sprintf("logDir=%q", logDir), func(t *testing.T) {
			cfg := configuredCheckpointFactoryTestConfig(t)
			cfg.FsstoreConfig.Layout.LogDir = logDir
			pattern := logpattern.FileNamePattern{
				Dir: filepath.Join(cfg.FsstoreConfig.Layout.LogPath, "explicit"), Target: "app", Extension: "log",
			}
			manager := NewFileSeriesManager(FileSeriesManagerOptions{
				Config: appendlog.NewConfigAdapter(cfg.FsstoreConfig), FileName: pattern,
			})
			lifecycle, err := NewDirectoryLifecycle(manager)
			require.NoError(t, err)
			require.NotNil(t, lifecycle)
			effectivePattern, err := manager.fileNamePattern()
			require.NoError(t, err)
			require.Equal(t, pattern, effectivePattern)
			entries, readErr := os.ReadDir(cfg.FsstoreConfig.Layout.LogPath)
			require.NoError(t, readErr)
			require.Empty(t, entries, "explicit FileName must not create its directory or use Config.LogDir")
		})
	}
}
