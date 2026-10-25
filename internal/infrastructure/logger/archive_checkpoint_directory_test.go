package logger

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	commoncheckpoint "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestLoggerContractsUseOptionalCheckpointDirectoryAndKeepFallbackFilesLazy(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprintf("prepared=%t", prepared), func(t *testing.T) {
			checkpointDirectory := filepath.Join(t.TempDir(), "checkpoints")
			if prepared {
				require.NoError(t, commoncheckpoint.PrepareDirectory(checkpointDirectory))
			}
			for _, contract := range []string{"sync", "async", "delivery"} {
				for _, dedicated := range []bool{false, true} {
					name := contract + "-base-fallback"
					if dedicated {
						name = contract + "-dedicated-fallback"
					}
					t.Run(name, func(t *testing.T) {
						t.Parallel()
						checkpoint := pkgfsstore.ArchiveCheckpointConfig{
							Directory: checkpointDirectory, SourcePrefix: name,
						}
						var logRoot string
						var fallback logging.LoggerErrorHandler
						if contract == "delivery" {
							cfg, _, _ := newHandlerOnlyDeliveryConfig(t, pkglogger.LogOutputFile)
							cfg.File.LifecycleRunnerSettings.ArchiveCheckpointConfig = checkpoint
							logRoot = cfg.File.FsstoreConfig.Layout.LogPath
							if dedicated {
								internalFile := *cfg.File
								internalFile.FsstoreConfig.Layout.LogPath = t.TempDir()
								cfg.InternalLogDestinations = &pkglogger.DeliveryInternalLogDestinationConfig{File: &internalFile}
								logRoot = internalFile.FsstoreConfig.Layout.LogPath
							}
							created, err := NewDeliveryLogger(context.Background(), cfg, streamrouting.None, observer.NopTracer{}, observer.NopMeter{})
							require.NoError(t, err)
							t.Cleanup(func() { require.NoError(t, created.Close()) })
							owned := created.(*topLevelDeliveryLoggerRuntime)
							fallback = owned.internalLogDestinations.fileLoggerSetForHandlers().NewNamedErrorFallbackHandler().Handler
						} else {
							cfg, _, _ := newHandlerOnlyLoggerConfig(t, pkglogger.LogOutputFile)
							cfg.File.ArchiveCheckpointConfig = checkpoint
							logRoot = cfg.File.FsstoreConfig.Layout.LogPath
							if dedicated {
								internalFile := *cfg.File
								internalFile.FsstoreConfig.Layout.LogPath = t.TempDir()
								cfg.InternalLogDestinations = &pkglogger.InternalLogDestinationConfig{File: &internalFile}
								logRoot = internalFile.FsstoreConfig.Layout.LogPath
							}
							if contract == "sync" {
								created, err := NewLogger(context.Background(), cfg, streamrouting.None, nil, nil, observer.NopTracer{}, observer.NopMeter{})
								require.NoError(t, err)
								t.Cleanup(func() { require.NoError(t, created.Close()) })
								owned := created.(*topLevelLoggerRuntime)
								fallback = owned.internalLogDestinations.fileLoggerSetForHandlers().NewNamedErrorFallbackHandler().Handler
							} else {
								asyncCfg := asyncConfigFromLoggerConfig(cfg)
								asyncCfg.MaxLogEntryBytes = cfg.MaxLogEntryBytes
								asyncCfg.MaxParallel = 1
								created, err := NewAsyncLogger(context.Background(), asyncCfg, streamrouting.None, nil, nil, observer.NopTracer{}, observer.NopMeter{})
								require.NoError(t, err)
								t.Cleanup(func() { require.NoError(t, created.Close()) })
								owned := created.(*topLevelAsyncLoggerRuntime)
								fallback = owned.internalLogDestinations.fileLoggerSetForHandlers().NewNamedErrorFallbackHandler().Handler
							}
						}
						require.Empty(t, logFilesUnder(t, logRoot), "sharing a prepared checkpoint directory must preserve lazy file opening")
						if prepared {
							entries, err := os.ReadDir(checkpointDirectory)
							require.NoError(t, err)
							require.Empty(t, entries, "initialization must not invent checkpoint history")
						} else {
							require.NoDirExists(t, checkpointDirectory)
						}
						fallback(context.Background(), errors.New("test failure"), &logging.LogEntry{
							Level: pkglogger.LogLevelError, Message: name,
						}, observer.NopTracer{}, observer.NopMeter{})
						require.NotEmpty(t, logFilesUnder(t, logRoot), "the first fallback event must open its file")
						if !prepared {
							require.NoDirExists(t, checkpointDirectory, "upload disabled must not create a directory on the first write either")
						}
					})
				}
			}
		})
	}
}
