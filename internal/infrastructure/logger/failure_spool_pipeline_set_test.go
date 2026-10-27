package logger

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestFailureSpoolPipelineSetRejectsMissingWriterAndReleasesLeaseOnCleanup(t *testing.T) {
	t.Parallel()

	cfg := testConstructibleOpenSearchFailureSpoolLoggerConfig(t.TempDir(), "opensearch")
	plan, err := buildFailureSpoolPipelinePlan([]failureSpoolPipelineSource{{
		contractKind: pkglogger.LoggerContractSync,
		config:       cfg,
	}})
	require.NoError(t, err)
	require.Len(t, plan, 1)

	pipelines, err := newFailureSpoolPipelineSet(
		context.Background(),
		plan,
		testFailureSpoolRuntimeConfig(),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	validationErr := pipelines.validateComplete()
	require.ErrorIs(t, validationErr, ErrFailureSpoolPipelineWriterCount)
	var pipelineErr *FailureSpoolPipelineError
	require.ErrorAs(t, validationErr, &pipelineErr)
	require.Equal(t, pkglogger.LoggerContractSync, pipelineErr.ContractKind)
	require.Equal(t, pkglogger.LogOutputOpenSearch, pipelineErr.Backend)
	require.Zero(t, pipelineErr.Writers)

	constructionErr := errors.New("construct logger")
	err = closeFailureSpoolPipelinesAfterConstructionError(constructionErr, pipelines)
	require.ErrorIs(t, err, constructionErr)

	replacement, acquireErr := directorylease.Acquire(
		"test.failure_spool.missing_writer_cleanup",
		plan[0].root,
	)
	require.NoError(t, acquireErr)
	replacement.Release()
}

func TestFailureSpoolPipelineSetValidationReportsEachMissingPart(t *testing.T) {
	t.Parallel()

	// writerを二つ持ち、runnerとLeaseを持たないpipelineを直接組み立て、各不備が
	// 発生元pipelineとwriter数を持つerrorとして報告されることを確認します。
	pipelines := &failureSpoolPipelineSet{pipelines: []*failureSpoolPipeline{{
		id: failureSpoolPipelineID{
			contractKind: pkglogger.LoggerContractAsyncBestEffort,
			backend:      pkglogger.LogOutputLoki,
		},
		writers: []*failureSpoolOwnedWriter{{}, {}},
	}}}

	pipelineErrs := joinedErrorsOfType[*FailureSpoolPipelineError](pipelines.validateComplete())
	require.Len(t, pipelineErrs, 3)
	for index, kind := range []error{
		ErrFailureSpoolPipelineWriterCount,
		ErrFailureSpoolPipelineRunnerMissing,
		ErrFailureSpoolPipelineLeaseMissing,
	} {
		require.ErrorIs(t, pipelineErrs[index], kind)
		require.Equal(t, pkglogger.LoggerContractAsyncBestEffort, pipelineErrs[index].ContractKind)
		require.Equal(t, pkglogger.LogOutputLoki, pipelineErrs[index].Backend)
	}
	require.Equal(t, 2, pipelineErrs[0].Writers)
}

func TestFailureSpoolPipelineSetRejectsWriterRegistrationAfterConstruction(t *testing.T) {
	t.Parallel()

	created, err := NewLogger(
		context.Background(),
		testConstructibleOpenSearchFailureSpoolLoggerConfig(t.TempDir(), "opensearch"),
		pkgcloudwatchlogs.StreamRoutingNone,
		testFailureSpoolRuntimeConfig(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	owned := created.(*topLevelLoggerRuntime)
	pipeline := owned.failureSpoolPipelines.pipelines[0]

	err = owned.failureSpoolPipelines.registerWriter(
		pkglogger.LoggerContractSync,
		pkglogger.LogOutputOpenSearch,
		pipeline.writers[0].handler,
	)
	require.ErrorIs(t, err, ErrFailureSpoolWriterRegistrationClosed)
	var pipelineErr *FailureSpoolPipelineError
	require.ErrorAs(t, err, &pipelineErr)
	require.Equal(t, pkglogger.LoggerContractSync, pipelineErr.ContractKind)
	require.Equal(t, pkglogger.LogOutputOpenSearch, pipelineErr.Backend)
	require.Len(t, pipeline.writers, 1)
	require.NoError(t, created.(logging.CloseableLogger).Close())
}

func TestConstructionRollbackKeepsPipelineWhenBackendCleanupIsIncomplete(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	plan := []failureSpoolPipelinePlan{
		testFailureSpoolPipelinePlan(
			pkglogger.LoggerContractSync,
			pkglogger.LogOutputOpenSearch,
			root,
		),
	}
	leases, err := acquireFailureSpoolDirectoryLeases(plan)
	require.NoError(t, err)
	pipelines := &failureSpoolPipelineSet{
		runtime:       &FailureSpoolRuntime{},
		leases:        leases,
		writersClosed: true,
	}
	t.Cleanup(func() {
		_, _ = pipelines.close()
	})

	cleanupErr := markLoggerOwnedResourceCleanupIncomplete(errors.New("backend drain failed"))
	err = closeOwnedLoggerResourcesWithError(cleanupErr, pipelines, nil, nil, nil)
	require.ErrorIs(t, err, errLoggerOwnedResourceCleanupIncomplete)

	conflicting, acquireErr := directorylease.Acquire(
		"test.failure_spool.incomplete_backend_cleanup",
		root,
	)
	require.Nil(t, conflicting)
	require.ErrorIs(t, acquireErr, directorylease.ErrDirectoryLeaseConflict)
}
