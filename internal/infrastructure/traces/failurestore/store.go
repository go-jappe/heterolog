// Package failurestoreは、traceの送り先へ送れなかったspanを、ログの出力先と同じ失敗処理
// （spool、failure archive、degrade、overload）へ渡し、spoolに保存したspanを送り直します。
// spanは1件ずつOTLP/JSONのrecordにして保存し、送り直すときにspanへ戻します。
//
// 配送の仕組みは自分の送信のspanを作らないため、使う部品にはspanを作らないTracerを
// 渡します。失敗処理の内部ログは、Telemetryのerror handlerへ渡します。
//
// failure archiveをobject storageへ送る場合、送れなかった圧縮fileを保存するspoolと
// archiveは、storageowner.Telemetryの名前で、loggerとは別の保存先を使います。
package failurestore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	fsstorespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	logspool "github.com/go-jappe/heterolog/internal/infrastructure/spool/log"
	"github.com/go-jappe/heterolog/internal/infrastructure/storageowner"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	objectstoragevalidation "github.com/go-jappe/heterolog/pkg/objectstorage/validation"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgspoolrunner "github.com/go-jappe/heterolog/pkg/spool/spoolrunner"
	pkgspoolsupervisor "github.com/go-jappe/heterolog/pkg/spool/spoolsupervisor"
)

// operationは、保存するrecordのoperationです。
const operation = "export_traces"

// Configは、failure storeの設定です。値の範囲は、pkg/telemetryの検証と、ログの出力先と
// 同じ構築時の検証で確かめます。
type Config struct {
	// Storeは、ログの出力先と同じfailure storeの設定です。
	Store pkgdurable.FailureStoreConfig
	// ReplayとReplaySupervisorは、spoolから送り直すrunnerと、その再起動の設定です。
	Replay           pkgspoolrunner.BackendSpoolRunnerConfig
	ReplaySupervisor pkgspoolsupervisor.RunnerSupervisorConfig
}

// Optionsは、Newに渡す依存です。
type Options struct {
	// Backendは、保存するrecordと、spool・archiveのmetricsのbackend属性に使う名前です。
	// Retry-Afterの期限を保存するfileの名前にもなるため、英小文字・数字・_・-だけにします。
	Backend string
	// Exporterは、spoolのspanを送り直す送り先です。Storeを止めるまで閉じてはいけません。
	Exporter traces.Exporter
	// Resourceは、Resourceを持たないspanを保存するときに付けるResourceです。
	Resource *tracing.Resource
	Clock    clock.Clock
	Meter    pkgobserver.Meter
	// Reportは、保存できなかったspanのほかに、失敗処理の内部ログを受け取ります。
	Report traces.ErrorHandler
}

// Storeは、traces.FailureStoreの実装です。
type Store struct {
	backend   string
	resource  *tracing.Resource
	clock     clock.Clock
	composite *flushfailure.CompositeFailureHandler
	// writerは、spoolを選んだ場合のspoolへの保存です。compositeは借りて使うだけのため、
	// compositeを止めた後にStoreが止めます。
	writer *spoolstorehandler.SpoolStoreHandler
	replay *replayRuntime
	// uploadSpoolsは、failure archiveの圧縮fileを送れなかった場合に保存するspoolです。
	// failure archiveが借りて使うため、compositeを止めた後にStoreが閉じます。
	uploadSpools compressedlog.RootRuntimes
}

// Newは、cfgで選んだfailure storeを作ります。spoolを選んだ場合は、同じspoolから送り直す
// runnerも始めます。何も保存しない設定（DurableTypeがnoneで、degradeもしない）なら、
// nilを返します。
func New(ctx context.Context, cfg Config, opts Options) (*Store, error) {
	if opts.Clock == nil {
		opts.Clock = clock.NewUTCClock()
	}
	opts.Meter = observer.EnsureMeter(opts.Meter)
	report := opts.Report
	s := &Store{backend: opts.Backend, resource: opts.Resource, clock: opts.Clock}
	cfg.Store = ownArchiveUploadStorage(cfg.Store)
	uploadSpools, err := prepareArchiveUpload(cfg.Store)
	if err != nil {
		return nil, err
	}
	s.uploadSpools = uploadSpools
	composite, err := flushfailure.Build(ctx, opts.Clock, loggerdurable.NewFailureStoreConfigAdapter(cfg.Store),
		nil, observer.NopTracer{}, opts.Meter, flushfailure.FactoryOptions{
			Backend:                   opts.Backend,
			InternalLogHandlerFactory: internalHandlers(report),
			SpoolHandlerRegistrar: func(writer *spoolstorehandler.SpoolStoreHandler) error {
				s.writer = writer
				return nil
			},
			CompressedLogSpoolRuntimes: uploadSpools,
		})
	s.composite = composite
	if err != nil {
		return nil, errors.Join(err, s.Shutdown(context.WithoutCancel(ctx)))
	}
	if composite == nil {
		return nil, s.uploadSpools.Close()
	}
	if s.writer != nil {
		replay, err := startReplay(cfg, opts, s.writer)
		if err != nil {
			return nil, errors.Join(err, s.Shutdown(context.WithoutCancel(ctx)))
		}
		s.replay = replay
	}
	return s, nil
}

// Storeは、batchのうちfailuresの範囲のspanを、1件ずつrecordにして保存します。
func (s *Store) Store(ctx context.Context, batch []tracing.SpanData, failures []traces.SpanFailure) error {
	now := s.clock.Now()
	result := loggerfailure.SendResult{Meta: loggerfailure.Meta{
		Backend:     s.backend,
		AttemptedAt: now,
		InputCount:  len(batch),
	}}
	for _, failure := range failures {
		for _, span := range batch[failure.Start:failure.End] {
			payload, err := encodePayload(span, s.resource)
			if err != nil {
				return err
			}
			record := loggerfailure.FailedRecord{
				Cause:          failure.Err,
				DeliveryStatus: deliveryStatus(failure),
				FailedAt:       now,
				Operation:      operation,
				StatusCode:     failure.StatusCode,
				FailureCode:    failureCode(failure),
				FailureReason:  errorText(failure.Err),
				Payload:        payload,
			}
			switch failure.Disposition {
			case traces.DispositionDead:
				result.DeadRecords = append(result.DeadRecords, record)
			case traces.DispositionQuarantine:
				result.QuarantineRecords = append(result.QuarantineRecords, record)
			default:
				record.RetryAfter = failure.RetryAfter
				result.RetryableRecords = append(result.RetryableRecords, record)
			}
		}
	}
	return s.composite.HandleFailure(ctx, result)
}

// Shutdownは、受け付けた保存を終えてからfailure storeを止め、送り直すrunnerを止めます。
// runnerは、開始済みの一周期を終えてから止まります。ctxが終わった場合は、開始済みの
// 処理を中止させ、待つのをやめてerrorを返します。
func (s *Store) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	var errs []error
	if s.composite != nil {
		errs = append(errs, s.composite.Drain(ctx))
	}
	if s.writer != nil {
		errs = append(errs, s.writer.Shutdown(ctx))
	}
	if s.replay != nil {
		errs = append(errs, s.replay.stop(ctx))
	}
	if s.uploadSpools != nil {
		errs = append(errs, s.uploadSpools.Close())
	}
	return errors.Join(errs...)
}

// ownArchiveUploadStorageは、failure archiveの圧縮fileを送れなかった場合に保存するspoolと
// archiveの所有者を、設定にかかわらずstorageowner.Telemetryにします。同じprocessのloggerの
// 保存先とぶつからないようにするためです。
func ownArchiveUploadStorage(cfg pkgdurable.FailureStoreConfig) pkgdurable.FailureStoreConfig {
	upload := &cfg.DurableStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig
	upload.CompressedLogFileUploadFailureStore.DurableStoreConfig.FileArchiveConfig.LoggerContract = storageowner.Telemetry.String()
	if upload.CompressedLogFileUploadSpool != nil {
		upload.CompressedLogFileUploadSpool = telemetryOwnedSpool{upload.CompressedLogFileUploadSpool}
	}
	return cfg
}

// telemetryOwnedSpoolは、圧縮ログのspoolの設定のうち、所有者だけをTelemetryにします。
type telemetryOwnedSpool struct {
	objectstorage.CompressedLogFileUploadSpoolConfigProvider
}

func (telemetryOwnedSpool) LoggerContract() string { return storageowner.Telemetry.String() }

// prepareArchiveUploadは、failure archiveをobject storageへ送る場合に、ログの出力先と同じく、
// 送信の設定を確かめ、checkpointの保存先を用意し、送れなかった圧縮fileを保存するspoolを
// 作ります。送らない場合は、何もせずにnilを返します。
func prepareArchiveUpload(cfg pkgdurable.FailureStoreConfig) (compressedlog.RootRuntimes, error) {
	if !slices.Contains(cfg.DurableType, pkgdurable.DurableTypeFileArchive) {
		return nil, nil
	}
	archive := cfg.DurableStoreConfig.FileArchiveConfig
	upload := compressedlog.FailureArchiveUpload(archive)
	if !upload.Enabled {
		return nil, nil
	}
	if err := objectstoragevalidation.ValidateUploadTargetConfig("trace failure archive", archive.ObjectStorageUploadConfig); err != nil {
		return nil, err
	}
	if err := checkpoint.PrepareDirectory(archive.ArchiveCheckpointConfig.Directory); err != nil {
		return nil, fmt.Errorf("prepare the checkpoint directory of the trace failure archive: %w", err)
	}
	configs, err := compressedlog.UploadConfigs(upload)
	if err != nil || configs == nil {
		return nil, err
	}
	return compressedlog.NewRootRuntimes(configs...)
}

// deliveryStatusは、受信側が受け付けたかについて分かっていることです。
func deliveryStatus(failure traces.SpanFailure) pkglogger.DeliveryStatus {
	if failure.StatusCode != 0 || failure.Disposition != traces.DispositionRetry {
		return pkglogger.DeliveryFailed
	}
	return pkglogger.DeliveryUnconfirmed
}

func failureCode(failure traces.SpanFailure) loggerfailure.FailureCode {
	if failure.Reason == "" {
		return "export_failed"
	}
	return loggerfailure.FailureCode(failure.Reason)
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// replayRuntimeは、spoolから送り直すrunnerの実行と、spoolのdirectoryの使用権です。
type replayRuntime struct {
	cancel      context.CancelFunc
	forceCancel context.CancelFunc
	done        chan struct{}

	stopOnce sync.Once
}

// startReplayは、writerと同じspoolから送り直すrunnerを作って始めます。writerが受け取った
// Retry-Afterは、runnerの送り先全体の待ち期限として記録し、同じ上限で短くします。
func startReplay(cfg Config, opts Options, writer *spoolstorehandler.SpoolStoreHandler) (*replayRuntime, error) {
	spoolCfg := cfg.Store.DurableStoreConfig.SpoolConfig
	root, err := fsstorespool.NewRootDir(spoolCfg.SpoolPath, spoolCfg.Target, spoolCfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("resolve the trace failure spool: %w", err)
	}
	scope := directorylease.ScopeLabel("telemetry." + opts.Backend + ".failure_spool")
	lease, err := directorylease.Acquire(scope, root.String())
	if err != nil {
		return nil, fmt.Errorf("acquire %s root %q: %w", scope, root, err)
	}
	report := opts.Report
	supervisor, err := logspool.NewBackendRunnerSupervisor(opts.Clock, logspool.BackendRuntimeOptions{
		Backend:          opts.Backend,
		SpoolConfig:      spoolCfg,
		RunnerConfig:     cfg.Replay,
		SupervisorConfig: cfg.ReplaySupervisor,
		Sender:           replaySender{exporter: opts.Exporter, clock: opts.Clock},
		OnWarn:           warnHandler(report),
	}, observer.NopTracer{}, opts.Meter)
	if err != nil {
		lease.Release()
		return nil, fmt.Errorf("build the trace failure spool runner: %w", err)
	}
	writer.SetMaxRetryAfter(cfg.Replay.Worker.MaxRetryAfter)
	if deadline := supervisor.Runner.RetryAfterDeadline(); deadline != nil {
		writer.SetRetryAfterRecorder(deadline)
	}
	supervisor.Reporter = runReporter{backend: opts.Backend, report: report}

	runCtx, cancel := context.WithCancel(context.Background())
	operationCtx, forceCancel := context.WithCancel(context.Background())
	r := &replayRuntime{cancel: cancel, forceCancel: forceCancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		defer lease.Release()
		err := supervisor.RunUntilStopped(runCtx, operationCtx)
		if err = logspool.NotificationError(runCtx, err); err != nil {
			report(fmt.Errorf("the %s failure spool runner stopped: %w", opts.Backend, err))
		}
	}()
	return r, nil
}

// stopは、runnerに止まるよう求め、止まるのを待ちます。ctxが終わった場合は、開始済みの
// 処理も中止させ、待つのをやめます。directoryの使用権は、runnerが止まってから手放します。
func (r *replayRuntime) stop(ctx context.Context) error {
	r.stopOnce.Do(r.cancel)
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		r.forceCancel()
		return fmt.Errorf("stop the trace failure spool runner: %w", ctx.Err())
	}
}
