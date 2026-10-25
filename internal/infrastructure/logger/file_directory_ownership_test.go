package logger

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestNewLoggerRejectsDirectoryOwnedByAnotherTopLevelLogger(t *testing.T) {
	fileCfg := newActivationFileConfig(t, "top-level-directory-owner")
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
		Timeout:               time.Second,
		MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
		File:                  fileCfg,
	}
	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}

	first, err := NewLogger(ctx, cfg, streamrouting.None, nil, nil, tracer, meter)
	require.NoError(t, err)

	second, err := NewLogger(ctx, cfg, streamrouting.None, nil, nil, tracer, meter)
	require.Nil(t, second)
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)
	var conflict *directorylease.ConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, syncFileDirectoryScopeLabel, conflict.RequestedScopeLabel)
	require.Equal(t, syncFileDirectoryScopeLabel, conflict.ExistingScopeLabel)
	require.NotZero(t, conflict.RequestedAttemptID)
	require.NotZero(t, conflict.ExistingLeaseID)
	require.NotEqual(t, conflict.ExistingLeaseID, conflict.RequestedAttemptID)

	require.NoError(t, closeSyncLogger(first))
	replacement, err := NewLogger(ctx, cfg, streamrouting.None, nil, nil, tracer, meter)
	require.NoError(t, err)
	require.NoError(t, closeSyncLogger(replacement))
}

func TestNewLoggerAllowsDifferentDirectory(t *testing.T) {
	root := t.TempDir()
	firstFile := newActivationFileConfig(t, "top-level-first")
	firstFile.FsstoreConfig.Layout.LogPath = filepath.Join(root, "first")
	secondFile := newActivationFileConfig(t, "top-level-second")
	secondFile.FsstoreConfig.Layout.LogPath = filepath.Join(root, "second")

	first, err := NewLogger(
		context.Background(),
		pkglogger.LoggerConfig{
			LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
			Timeout:               time.Second,
			MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
			File:                  firstFile,
		},
		streamrouting.None,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	require.NoError(t, err)
	defer func() { require.NoError(t, closeSyncLogger(first)) }()

	second, err := NewLogger(
		context.Background(),
		pkglogger.LoggerConfig{
			LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
			Timeout:               time.Second,
			MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
			File:                  secondFile,
		},
		streamrouting.None,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	require.NoError(t, err)
	require.NoError(t, closeSyncLogger(second))
}

func TestTopLevelLoggerModesCannotShareFileDirectory(t *testing.T) {
	fileCfg := newActivationFileConfig(t, "cross-mode-directory-owner")
	syncLogger, err := NewLogger(
		context.Background(),
		pkglogger.LoggerConfig{
			LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
			Timeout:               time.Second,
			MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
			File:                  fileCfg,
		},
		streamrouting.None,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	require.NoError(t, err)
	defer func() { require.NoError(t, closeSyncLogger(syncLogger)) }()

	asyncCfg := pkglogger.AsyncBestEffortMultiLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
		Timeout:               time.Second,
		MaxParallel:           1,
		MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
		File:                  fileCfg,
	}
	asyncLogger, err := NewAsyncLogger(
		context.Background(),
		asyncCfg,
		streamrouting.None,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	require.Nil(t, asyncLogger)
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)
	var conflict *directorylease.ConflictError
	require.True(t, errors.As(err, &conflict))
	require.Equal(t, asyncFileDirectoryScopeLabel, conflict.RequestedScopeLabel)
	require.Equal(t, syncFileDirectoryScopeLabel, conflict.ExistingScopeLabel)
	require.NotZero(t, conflict.RequestedAttemptID)
	require.NotZero(t, conflict.ExistingLeaseID)
	require.NotEqual(t, conflict.ExistingLeaseID, conflict.RequestedAttemptID)
}

func TestAsyncLoggerRejectsDuplicateDirectoryAndReleasesItOnClose(t *testing.T) {
	fileCfg := newActivationFileConfig(t, "async-directory-owner")
	cfg := pkglogger.AsyncBestEffortMultiLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
		File:                  fileCfg,
		Timeout:               time.Second,
		MaxParallel:           1,
		MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
	}

	first, err := NewAsyncLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	require.NoError(t, err)

	second, err := NewAsyncLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	require.Nil(t, second)
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)

	require.NoError(t, first.(applogging.AsyncCloseableLogger).Close())
	replacement, err := NewAsyncLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	require.NoError(t, err)
	require.NoError(t, replacement.(applogging.AsyncCloseableLogger).Close())
}

func TestDeliveryLoggerRejectsDuplicateDirectoryAndReleasesItOnClose(t *testing.T) {
	fileCfg := newCloseOwnershipDeliveryFileConfig(t.TempDir(), "delivery-directory-owner")
	cfg := pkglogger.DeliveryLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
		Timeout:               time.Second,
		MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
		File:                  fileCfg,
	}

	first, err := NewDeliveryLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)

	second, err := NewDeliveryLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.Nil(t, second)
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)
	var conflict *directorylease.ConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, deliveryFileDirectoryScopeLabel, conflict.RequestedScopeLabel)
	require.Equal(t, deliveryFileDirectoryScopeLabel, conflict.ExistingScopeLabel)
	require.NotZero(t, conflict.RequestedAttemptID)
	require.NotZero(t, conflict.ExistingLeaseID)
	require.NotEqual(t, conflict.ExistingLeaseID, conflict.RequestedAttemptID)

	require.NoError(t, first.(applogging.DeliveryClosableLogger).Close())
	replacement, err := NewDeliveryLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	require.NoError(t, replacement.(applogging.DeliveryClosableLogger).Close())
}

func TestNewLoggerReleasesBaseDirectoryWhenLaterConstructionFails(t *testing.T) {
	fileCfg := newActivationFileConfig(t, "constructor-rollback-directory-owner")
	invalid := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
		Timeout:               time.Second,
		MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
		File:                  fileCfg,
		InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{
			// 専用internal-log loggerを同じpathへ重ね、base Set取得後に失敗させます。
			File: fileCfg,
		},
	}

	created, err := NewLogger(
		context.Background(),
		invalid,
		streamrouting.None,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	require.Nil(t, created)
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)

	valid := invalid
	valid.InternalLogDestinations = nil
	replacement, err := NewLogger(
		context.Background(),
		valid,
		streamrouting.None,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	require.NoError(t, err)
	require.NoError(t, closeSyncLogger(replacement))
}

func TestRunnerModeSecondLoggerFailsBeforeCreatingAnotherRunner(t *testing.T) {
	fileCfg := runnerFileConfigForInternalLogRuntime(
		t.TempDir(),
		"runner-directory-owner",
	)
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
		Timeout:               time.Second,
		MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
		File:                  fileCfg,
	}

	first, err := NewLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	require.NoError(t, err)
	owned := first.(*topLevelLoggerRuntime)
	require.NotNil(t, owned.appendLogMaintenanceRuntime)
	require.NotEmpty(t, owned.appendLogMaintenanceRuntime.entries)

	second, err := NewLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	require.Nil(t, second)
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)
	require.NoError(t, owned.Close())
}

func closeSyncLogger(logger applogging.Logger) error {
	if logger == nil {
		return nil
	}
	closeable, ok := logger.(applogging.CloseableLogger)
	if !ok {
		return nil
	}
	return closeable.Close()
}
