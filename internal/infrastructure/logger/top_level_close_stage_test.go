package logger

import (
	"errors"
	"testing"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"

	"github.com/stretchr/testify/require"
)

func TestTopLevelLoggerCloseStageZeroValueReachesCompleted(t *testing.T) {
	t.Run("sync", func(t *testing.T) {
		backend := &closeStageTestSyncBackend{}
		runtime := &topLevelLoggerRuntime{topLevelLoggerDelegate: backend}

		require.Equal(t, topLevelLoggerCloseStageAppendLogMaintenance, runtime.closeStage)
		require.NoError(t, runtime.Close())
		require.Equal(t, topLevelLoggerCloseStageCompleted, runtime.closeStage)
		require.Equal(t, 1, backend.closeCalls)

		require.NoError(t, runtime.Close())
		require.Equal(t, topLevelLoggerCloseStageCompleted, runtime.closeStage)
		require.Equal(t, 1, backend.closeCalls)
	})

	t.Run("async", func(t *testing.T) {
		backend := &closeStageTestAsyncBackend{}
		runtime := &topLevelAsyncLoggerRuntime{topLevelAsyncLoggerDelegate: backend}

		require.Equal(t, topLevelLoggerCloseStageAppendLogMaintenance, runtime.closeStage)
		require.NoError(t, runtime.Close())
		require.Equal(t, topLevelLoggerCloseStageCompleted, runtime.closeStage)
		require.Equal(t, 1, backend.closeCalls)

		require.NoError(t, runtime.Close())
		require.Equal(t, topLevelLoggerCloseStageCompleted, runtime.closeStage)
		require.Equal(t, 1, backend.closeCalls)
	})

	t.Run("delivery", func(t *testing.T) {
		backend := &closeStageTestDeliveryBackend{}
		runtime := &topLevelDeliveryLoggerRuntime{topLevelDeliveryLoggerDelegate: backend}

		require.Equal(t, topLevelLoggerCloseStageAppendLogMaintenance, runtime.closeStage)
		require.NoError(t, runtime.Close())
		require.Equal(t, topLevelLoggerCloseStageCompleted, runtime.closeStage)
		require.Equal(t, 1, backend.closeCalls)

		require.NoError(t, runtime.Close())
		require.Equal(t, topLevelLoggerCloseStageCompleted, runtime.closeStage)
		require.Equal(t, 1, backend.closeCalls)
	})
}

func TestTopLevelLoggerCloseStageRetriesBackendBeforeAdvancing(t *testing.T) {
	backendErr := errors.New("backend close failed")

	t.Run("sync", func(t *testing.T) {
		backend := &closeStageTestSyncBackend{closeErrors: []error{backendErr, nil}}
		runtime := &topLevelLoggerRuntime{topLevelLoggerDelegate: backend}

		require.ErrorIs(t, runtime.closeAttempt(), backendErr)
		require.Equal(t, topLevelLoggerCloseStageBackendLogger, runtime.closeStage)
		require.Equal(t, 1, backend.closeCalls)

		require.NoError(t, runtime.closeAttempt())
		require.Equal(t, topLevelLoggerCloseStageCompleted, runtime.closeStage)
		require.Equal(t, 2, backend.closeCalls)

		require.NoError(t, runtime.closeAttempt())
		require.Equal(t, 2, backend.closeCalls)
	})

	t.Run("async", func(t *testing.T) {
		backend := &closeStageTestAsyncBackend{closeErrors: []error{backendErr, nil}}
		runtime := &topLevelAsyncLoggerRuntime{topLevelAsyncLoggerDelegate: backend}

		require.ErrorIs(t, runtime.closeAttempt(), backendErr)
		require.Equal(t, topLevelLoggerCloseStageBackendLogger, runtime.closeStage)
		require.Equal(t, 1, backend.closeCalls)

		require.NoError(t, runtime.closeAttempt())
		require.Equal(t, topLevelLoggerCloseStageCompleted, runtime.closeStage)
		require.Equal(t, 2, backend.closeCalls)

		require.NoError(t, runtime.closeAttempt())
		require.Equal(t, 2, backend.closeCalls)
	})

	t.Run("delivery", func(t *testing.T) {
		backend := &closeStageTestDeliveryBackend{closeErrors: []error{backendErr, nil}}
		runtime := &topLevelDeliveryLoggerRuntime{topLevelDeliveryLoggerDelegate: backend}

		require.ErrorIs(t, runtime.closeAttempt(), backendErr)
		require.Equal(t, topLevelLoggerCloseStageBackendLogger, runtime.closeStage)
		require.Equal(t, 1, backend.closeCalls)

		require.NoError(t, runtime.closeAttempt())
		require.Equal(t, topLevelLoggerCloseStageCompleted, runtime.closeStage)
		require.Equal(t, 2, backend.closeCalls)

		require.NoError(t, runtime.closeAttempt())
		require.Equal(t, 2, backend.closeCalls)
	})
}

// 公開Closeの二回目はshutdown.Controllerが保持する結果を返すだけなので、stageの保持は
// closeAttemptを直接呼んで検証します。production ControllerもbackendのClose失敗後に
// closeAttemptを再実行します。その再試行と完了後の呼び出しで、advance済みの
// appendlog maintenanceを閉じ直さず、最初の結果だけを報告しなければなりません。
func TestTopLevelLoggerCloseStageCachesMaintenanceErrorAfterAdvancing(t *testing.T) {
	maintenanceErr := errors.New("appendlog maintenance close failed")
	backendErr := errors.New("backend close failed")
	lateErr := errors.New("must not be collected by a repeated close attempt")

	t.Run("sync", func(t *testing.T) {
		maintenance := &appendLogMaintenanceRuntime{errs: []error{maintenanceErr}}
		backend := &closeStageTestSyncBackend{closeErrors: []error{backendErr}}
		runtime := &topLevelLoggerRuntime{
			topLevelLoggerDelegate:      backend,
			appendLogMaintenanceRuntime: maintenance,
		}

		err := runtime.closeAttempt()
		require.ErrorIs(t, err, maintenanceErr)
		require.ErrorIs(t, err, backendErr)
		require.Equal(t, topLevelLoggerCloseStageBackendLogger, runtime.closeStage)

		appendMaintenanceRuntimeError(maintenance, lateErr)
		err = runtime.closeAttempt()
		require.ErrorIs(t, err, maintenanceErr)
		require.NotErrorIs(t, err, lateErr)
		require.NotErrorIs(t, err, backendErr)
		require.Equal(t, topLevelLoggerCloseStageCompleted, runtime.closeStage)
		require.Equal(t, 2, backend.closeCalls)

		err = runtime.closeAttempt()
		require.ErrorIs(t, err, maintenanceErr)
		require.NotErrorIs(t, err, lateErr)
		require.Equal(t, 2, backend.closeCalls)
	})

	t.Run("async", func(t *testing.T) {
		maintenance := &appendLogMaintenanceRuntime{errs: []error{maintenanceErr}}
		backend := &closeStageTestAsyncBackend{closeErrors: []error{backendErr}}
		runtime := &topLevelAsyncLoggerRuntime{
			topLevelAsyncLoggerDelegate: backend,
			appendLogMaintenanceRuntime: maintenance,
		}

		err := runtime.closeAttempt()
		require.ErrorIs(t, err, maintenanceErr)
		require.ErrorIs(t, err, backendErr)
		require.Equal(t, topLevelLoggerCloseStageBackendLogger, runtime.closeStage)

		appendMaintenanceRuntimeError(maintenance, lateErr)
		err = runtime.closeAttempt()
		require.ErrorIs(t, err, maintenanceErr)
		require.NotErrorIs(t, err, lateErr)
		require.NotErrorIs(t, err, backendErr)
		require.Equal(t, topLevelLoggerCloseStageCompleted, runtime.closeStage)
		require.Equal(t, 2, backend.closeCalls)

		err = runtime.closeAttempt()
		require.ErrorIs(t, err, maintenanceErr)
		require.NotErrorIs(t, err, lateErr)
		require.Equal(t, 2, backend.closeCalls)
	})

	t.Run("delivery", func(t *testing.T) {
		maintenance := &appendLogMaintenanceRuntime{errs: []error{maintenanceErr}}
		backend := &closeStageTestDeliveryBackend{closeErrors: []error{backendErr}}
		runtime := &topLevelDeliveryLoggerRuntime{
			topLevelDeliveryLoggerDelegate: backend,
			appendLogMaintenanceRuntime:    maintenance,
		}

		err := runtime.closeAttempt()
		require.ErrorIs(t, err, maintenanceErr)
		require.ErrorIs(t, err, backendErr)
		require.Equal(t, topLevelLoggerCloseStageBackendLogger, runtime.closeStage)

		appendMaintenanceRuntimeError(maintenance, lateErr)
		err = runtime.closeAttempt()
		require.ErrorIs(t, err, maintenanceErr)
		require.NotErrorIs(t, err, lateErr)
		require.NotErrorIs(t, err, backendErr)
		require.Equal(t, topLevelLoggerCloseStageCompleted, runtime.closeStage)
		require.Equal(t, 2, backend.closeCalls)

		err = runtime.closeAttempt()
		require.ErrorIs(t, err, maintenanceErr)
		require.NotErrorIs(t, err, lateErr)
		require.Equal(t, 2, backend.closeCalls)
	})
}

// 完了後のcloseAttemptも、pipelineを閉じ直さず完了時のspool errorだけを報告します。
// 公開Closeの二回目はController保持の結果を返すだけなので、closeAttemptを直接呼びます。
func TestTopLevelLoggerCloseStageCachesCompletedFailureSpoolError(t *testing.T) {
	spoolErr := errors.New("failure spool runtime close failed")
	lateErr := errors.New("must not be collected after the pipeline completed")

	t.Run("sync", func(t *testing.T) {
		spoolRuntime := &FailureSpoolRuntime{errs: []error{spoolErr}}
		pipelines := &failureSpoolPipelineSet{
			runtime:       spoolRuntime,
			writersClosed: true,
		}
		backend := &closeStageTestSyncBackend{}
		runtime := &topLevelLoggerRuntime{
			topLevelLoggerDelegate: backend,
			failureSpoolPipelines:  pipelines,
		}

		err := runtime.Close()
		require.ErrorIs(t, err, spoolErr)
		require.Equal(t, topLevelLoggerCloseStageCompleted, runtime.closeStage)
		require.True(t, pipelines.runtimeClosed)
		require.True(t, pipelines.leasesReleased)
		require.Equal(t, 1, backend.closeCalls)

		appendFailureSpoolRuntimeError(spoolRuntime, lateErr)
		err = runtime.closeAttempt()
		require.ErrorIs(t, err, spoolErr)
		require.NotErrorIs(t, err, lateErr)
		require.Equal(t, 1, backend.closeCalls)
	})

	t.Run("async", func(t *testing.T) {
		spoolRuntime := &FailureSpoolRuntime{errs: []error{spoolErr}}
		pipelines := &failureSpoolPipelineSet{
			runtime:       spoolRuntime,
			writersClosed: true,
		}
		backend := &closeStageTestAsyncBackend{}
		runtime := &topLevelAsyncLoggerRuntime{
			topLevelAsyncLoggerDelegate: backend,
			failureSpoolPipelines:       pipelines,
		}

		err := runtime.Close()
		require.ErrorIs(t, err, spoolErr)
		require.Equal(t, topLevelLoggerCloseStageCompleted, runtime.closeStage)
		require.True(t, pipelines.runtimeClosed)
		require.True(t, pipelines.leasesReleased)
		require.Equal(t, 1, backend.closeCalls)

		appendFailureSpoolRuntimeError(spoolRuntime, lateErr)
		err = runtime.closeAttempt()
		require.ErrorIs(t, err, spoolErr)
		require.NotErrorIs(t, err, lateErr)
		require.Equal(t, 1, backend.closeCalls)
	})
}

func appendMaintenanceRuntimeError(runtime *appendLogMaintenanceRuntime, err error) {
	runtime.errMu.Lock()
	defer runtime.errMu.Unlock()
	runtime.errs = append(runtime.errs, err)
}

func appendFailureSpoolRuntimeError(runtime *FailureSpoolRuntime, err error) {
	runtime.errMu.Lock()
	defer runtime.errMu.Unlock()
	runtime.errs = append(runtime.errs, err)
}

// closeStageTest*BackendはClose呼出しの回数と時刻を記録します。時刻は再試行が間隔を
// 空けて行われたかを確認するためのものです。
type closeStageTestSyncBackend struct {
	applogging.Logger
	closeErrors []error
	closeCalls  int
	closeTimes  []time.Time
}

func (b *closeStageTestSyncBackend) StopAccepting() {}

func (b *closeStageTestSyncBackend) Close() error {
	return b.nextCloseResult()
}

func (b *closeStageTestSyncBackend) nextCloseResult() error {
	index := b.closeCalls
	b.closeCalls++
	b.closeTimes = append(b.closeTimes, time.Now())
	if index >= len(b.closeErrors) {
		return nil
	}
	return b.closeErrors[index]
}

type closeStageTestAsyncBackend struct {
	applogging.AsyncLogger
	closeErrors []error
	closeCalls  int
	closeTimes  []time.Time
}

func (b *closeStageTestAsyncBackend) StopAccepting() {}

func (b *closeStageTestAsyncBackend) Close() error {
	index := b.closeCalls
	b.closeCalls++
	b.closeTimes = append(b.closeTimes, time.Now())
	if index >= len(b.closeErrors) {
		return nil
	}
	return b.closeErrors[index]
}

type closeStageTestDeliveryBackend struct {
	applogging.DeliveryLogger
	closeErrors []error
	closeCalls  int
	closeTimes  []time.Time
}

func (b *closeStageTestDeliveryBackend) StopAccepting() {}

func (b *closeStageTestDeliveryBackend) Close() error {
	index := b.closeCalls
	b.closeCalls++
	b.closeTimes = append(b.closeTimes, time.Now())
	if index >= len(b.closeErrors) {
		return nil
	}
	return b.closeErrors[index]
}
