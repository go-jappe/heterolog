package flushfailure

import (
	"context"
	"errors"
	"fmt"

	spoolfsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/degradehandler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/fsarchivehandler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
	loggerhandler "github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// deliveryFailureHandlerMaxConcurrentは、Deliveryのfailure処理を一度に実行する上限です。
// Deliveryは各destinationの確定結果を同期的にreportへ含めるため、この値を外部設定にはせず、
// Delivery専用factoryが直列実行の方針として所有します。
const deliveryFailureHandlerMaxConcurrent = 1

// internalCompositeSemaphoreCapacityは、child構築開始前の設定errorと一緒に返す
// cleanup用の空Compositeだけが使用する内部capacityです。
const internalCompositeSemaphoreCapacity = 1

// FailureHandlerConcurrencyErrorは、failure処理を行う設定の同時実行数が不正であることを表します。
// Errはpkg/logger/durableのErrInvalidFailureHandlerMaxConcurrentを含む検証errorで、
// errors.Isで判別できます。
type FailureHandlerConcurrencyError struct {
	// MaxConcurrentは、設定から読み出して検証した同時実行数です。
	MaxConcurrent int
	Err           error
}

func (e *FailureHandlerConcurrencyError) Error() string {
	return fmt.Sprintf("invalid failure handler concurrency: %v", e.Err)
}

func (e *FailureHandlerConcurrencyError) Unwrap() error { return e.Err }

// file archiveの公開型に同期処理、Delivery report、終了処理の三つの契約が残ることを
// compile時に保証します。保存処理を内部handlerへ分離してもCompositeの配線先はPipelineです。
var (
	_ FailureHandler           = (*fsarchivehandler.FailureArchivePipeline)(nil)
	_ FailureHandlerWithReport = (*fsarchivehandler.FailureArchivePipeline)(nil)
	_ HandlerShutdown          = (*fsarchivehandler.FailureArchivePipeline)(nil)
)

// newMemoryPressureProviderは、監視を渡されなかったspoolが自ら起動する監視で、OSのmemoryを
// 読むproviderを作ります。試験では差し替えて、決まった値を返すproviderにします。
var newMemoryPressureProvider = memorypressure.New

// FactoryOptionsはinternal log routingとspool handlerのShutdown責任の移譲先です。
type FactoryOptions struct {
	InternalLogHandlerFactory InternalLogHandlerFactory
	SpoolHandlerRegistrar     SpoolHandlerRegistrar
	// CompressedLogSpoolRuntimesは圧縮済み.jsonlの送信失敗時に使う共有spoolです。
	// 所有者は各failure handlerの終了後にrootを解放します。
	CompressedLogSpoolRuntimes compressedlog.RootRuntimes
}

// Build は設定で選択されたspool/fileArchive handlerと、
// overload処理・観測用metadataを起動時に組み立てます。
// 保存方式と並列数はcfgから読み出し、個別の引数では受け取りません。
// DurableTypeNoneの場合は、EnableDegradeに応じて直接退避handlerまたはnilを返します。
// handler構築開始後のerrorでは先に構築できたhandlerを含むCompositeを返します。
// concurrencyや必須configを構築前に拒否した場合はnilを返します。呼び出し側はerrorで
// 起動を中止する場合も、戻り値が非nilなら先にDrainしなければなりません。
// SpoolHandlerRegistrarを指定した呼び出し側は、Drain完了後に登録済みspool handlerを
// 自らShutdownします。未指定の場合は従来どおりCompositeがShutdownまで所有します。
func Build(
	ctx context.Context,
	timeSource clock.Clock,
	cfg loggerdurable.FailureStoreConfigProvider,
	memoryMonitor memorypressure.Monitor,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	opts FactoryOptions,
) (*CompositeFailureHandler, error) {
	if cfg == nil {
		return nil, ErrNilFailureStoreConfig
	}
	durableTypes := cfg.DurableType()
	maxConcurrent := cfg.MaxConcurrent()
	if err := pkgdurable.ValidateFailureStoreDurableTypes(durableTypes); err != nil {
		return newFactoryErrorComposite(), fmt.Errorf(
			"invalid failure store durable types: %w",
			err,
		)
	}
	if durableTypes[0] == pkgdurable.DurableTypeNone {
		if !cfg.EnableDegrade() {
			return nil, nil
		}
		if err := pkgdurable.ValidateFailureHandlerMaxConcurrent(maxConcurrent); err != nil {
			return nil, &FailureHandlerConcurrencyError{MaxConcurrent: maxConcurrent, Err: err}
		}
		return buildDirectDegrade(
			maxConcurrent,
			cfg.SpillType(),
			meter,
		)
	}
	if err := pkgdurable.ValidateFailureHandlerMaxConcurrent(maxConcurrent); err != nil {
		return nil, &FailureHandlerConcurrencyError{MaxConcurrent: maxConcurrent, Err: err}
	}
	if cfg.DurableStoreConfig() == nil {
		return newFactoryErrorComposite(), errors.New("durable store config is nil")
	}

	storeCfg := cfg.DurableStoreConfig()
	handlers := make([]FailureHandler, 0, len(durableTypes))
	var errs []error

	for _, durableType := range durableTypes {
		switch durableType {
		case pkgdurable.DurableTypeSpool:
			spoolCfg := storeCfg.SpoolConfig()
			if spoolCfg == nil {
				errs = append(errs, errors.New("spool config is nil"))
				continue
			}
			spoolStore, err := spoolfsstore.NewSpoolStore(
				timeSource,
				spoolCfg,
				tracer,
				meter,
			)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			overloadHandler, err := spoolstorehandler.NewOverloadHandlerProvider(spoolCfg, meter)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to create spool overload handler: %w", err))
				continue
			}
			// 呼び出し元が監視を渡さなければ、spoolの閾値でOSのmemoryを読む監視を
			// このhandler用に起動し、handlerのShutdownで止めます。
			spoolMemoryMonitor := memoryMonitor
			var stopSpoolMemoryMonitor func()
			if spoolMemoryMonitor == nil {
				spoolMemoryMonitor, stopSpoolMemoryMonitor, err = spoolstorehandler.StartMemoryPressureMonitor(
					spoolCfg,
					newMemoryPressureProvider(),
				)
				if err != nil {
					errs = append(errs, err)
					continue
				}
			}
			internalHandlers := buildInternalHandlers(
				opts.InternalLogHandlerFactory,
				pkgdurable.DurableTypeSpool,
				spoolCfg.Handler(),
			)
			spooler := spoolstorehandler.NewSpoolStoreHandler(
				ctx,
				timeSource,
				spoolStore,
				spoolCfg,
				spoolMemoryMonitor,
				overloadHandler,
				tracer,
				meter,
				spoolstorehandler.InternalLogHandlers{
					OnError: internalHandlers.OnError,
					OnWarn:  internalHandlers.OnWarn,
					OnInfo:  internalHandlers.OnInfo,
					OnDebug: internalHandlers.OnDebug,
				},
			)
			spooler.StopMemoryMonitorOnShutdown(stopSpoolMemoryMonitor)
			if opts.SpoolHandlerRegistrar == nil {
				handlers = append(handlers, spooler)
				continue
			}
			if err := opts.SpoolHandlerRegistrar(spooler); err != nil {
				// 所有権移譲に失敗したraw handlerは部分構築Compositeへ残します。
				// callerはBuildのerror時にもCompositeをDrainする契約なので、Shutdownが
				// timeoutしても同じhandlerを失わず、次回のcleanupで再試行できます。
				handlers = append(handlers, spooler)
				errs = append(errs, fmt.Errorf("register spool handler: %w", err))
				continue
			}
			handlers = append(handlers, newBorrowedSpoolFailureHandler(spooler))

		case pkgdurable.DurableTypeFileArchive:
			archiveCfg := storeCfg.FileArchiveConfig()
			if archiveCfg == nil {
				errs = append(errs, fsarchivehandler.ErrNilFileArchiveConfig)
				continue
			}
			archivePipeline, err := newFailureArchivePipeline(
				ctx,
				timeSource,
				archiveCfg,
				tracer,
				meter,
				opts,
			)
			if archivePipeline != nil {
				handlers = append(handlers, archivePipeline)
			}
			if err != nil {
				errs = append(errs, err)
				continue
			}

		default:
			errs = append(errs, fmt.Errorf("unknown failure handler type: %s", durableType))
		}
	}

	return newCompositeFailureHandler(handlers, maxConcurrent), errors.Join(errs...)
}

// BuildDeliveryは、Delivery loggerで選択されたreport対応failure destinationを組み立てます。
// 現在選択できるdurable destinationはfile archiveだけですが、その制約と構築分岐を
// このfactoryに閉じ込め、backend固有の配線が具体的なdestinationへ依存しないようにします。
// 呼び出し側はHandleFailureWithReportを使い、failure処理の確定結果をDeliveryReportへ
// 反映してから送達処理を完了します。
// DurableTypeNone、直接退避、設定欠落はBuildと同じように区別します。
// error時にも部分構築済みCompositeを返す場合があり、その所有者はDrainする必要があります。
func BuildDelivery(
	ctx context.Context,
	timeSource clock.Clock,
	cfg loggerdurable.DeliveryFailureStoreConfigProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	opts FactoryOptions,
) (*ReportingCompositeFailureHandler, error) {
	if cfg == nil {
		return newFactoryErrorReportingComposite(), fmt.Errorf("delivery %w", ErrNilFailureStoreConfig)
	}
	durableTypes := cfg.DurableType()
	if err := pkgdurable.ValidateDeliveryFailureStoreDurableTypes(durableTypes); err != nil {
		return newFactoryErrorReportingComposite(), fmt.Errorf(
			"invalid delivery failure store durable types: %w",
			err,
		)
	}
	if durableTypes[0] == pkgdurable.DurableTypeNone {
		if !cfg.EnableDegrade() {
			return nil, nil
		}
		return buildDirectDegradeWithReport(
			deliveryFailureHandlerMaxConcurrent,
			cfg.SpillType(),
			meter,
		)
	}
	if cfg.DeliveryDurableStoreConfig() == nil {
		return newFactoryErrorReportingComposite(), errors.New("delivery durable store config is nil")
	}
	storeCfg := cfg.DeliveryDurableStoreConfig()
	handlers := make([]FailureHandlerWithReport, 0, len(durableTypes))
	var errs []error

	for _, durableType := range durableTypes {
		switch durableType {
		case pkgdurable.DurableTypeFileArchive:
			archiveCfg := storeCfg.FileArchiveConfig()
			if archiveCfg == nil {
				errs = append(errs, fsarchivehandler.ErrNilFileArchiveConfig)
				continue
			}
			archivePipeline, err := newFailureArchivePipeline(
				ctx,
				timeSource,
				archiveCfg,
				tracer,
				meter,
				opts,
			)
			if archivePipeline != nil {
				handlers = append(handlers, archivePipeline)
			}
			if err != nil {
				errs = append(errs, err)
				continue
			}

		default:
			errs = append(errs, fmt.Errorf("unsupported delivery failure handler type: %s", durableType))
		}
	}

	return newReportingCompositeFailureHandler(
		handlers,
		deliveryFailureHandlerMaxConcurrent,
	), errors.Join(errs...)
}

func buildDirectDegrade(
	maxConcurrent int,
	destinations []pkgdurable.SpillType,
	meter pkgobserver.Meter,
) (*CompositeFailureHandler, error) {
	if err := pkgdurable.ValidateDirectDegradeSpillTypes(destinations); err != nil {
		return newFactoryErrorComposite(), fmt.Errorf(
			"invalid direct degrade spill types: %w",
			err,
		)
	}

	degradeHandlers, err := newDirectDegradeHandlers(destinations, meter)
	handlers := make([]FailureHandler, 0, len(degradeHandlers))
	for _, handler := range degradeHandlers {
		handlers = append(handlers, handler)
	}

	return newCompositeFailureHandler(handlers, maxConcurrent), err
}

func buildDirectDegradeWithReport(
	maxConcurrent int,
	destinations []pkgdurable.SpillType,
	meter pkgobserver.Meter,
) (*ReportingCompositeFailureHandler, error) {
	if err := pkgdurable.ValidateDirectDegradeSpillTypes(destinations); err != nil {
		return newFactoryErrorReportingComposite(), fmt.Errorf(
			"invalid direct degrade spill types: %w",
			err,
		)
	}

	degradeHandlers, err := newDirectDegradeHandlers(destinations, meter)
	handlers := make([]FailureHandlerWithReport, 0, len(degradeHandlers))
	for _, handler := range degradeHandlers {
		handlers = append(handlers, handler)
	}

	return newReportingCompositeFailureHandler(handlers, maxConcurrent), err
}

// newFactoryErrorCompositeとnewFactoryErrorReportingCompositeは、child構築前の
// 設定errorと一緒に返す空Compositeです。この戻り値はDrainだけを許すcleanup handleであり、
// 設定されたMaxConcurrentを実行時defaultとして補正するものではありません。
func newFactoryErrorComposite() *CompositeFailureHandler {
	return newCompositeFailureHandler(nil, internalCompositeSemaphoreCapacity)
}

func newFactoryErrorReportingComposite() *ReportingCompositeFailureHandler {
	return newReportingCompositeFailureHandler(nil, internalCompositeSemaphoreCapacity)
}

func newDirectDegradeHandlers(
	destinations []pkgdurable.SpillType,
	meter pkgobserver.Meter,
) ([]*degradehandler.Handler, error) {
	handlers := make([]*degradehandler.Handler, 0, len(destinations))
	var errs []error
	for _, destination := range destinations {
		handler, err := degradehandler.New(destination, meter)
		if err != nil {
			errs = append(errs, fmt.Errorf(
				"failed to create direct degrade handler for %q: %w",
				destination,
				err,
			))
			continue
		}
		handlers = append(handlers, handler)
	}
	return handlers, errors.Join(errs...)
}

// newFailureArchivePipelineはfile archiveの内部ログ配線を解決し、failure保存用handlerと
// maintenance runnerを一体で終了できるPipelineを構築します。
func newFailureArchivePipeline(
	ctx context.Context,
	timeSource clock.Clock,
	cfg loggerdurable.FileArchiveConfigProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	opts FactoryOptions,
) (*fsarchivehandler.FailureArchivePipeline, error) {
	handlers := buildInternalHandlers(
		opts.InternalLogHandlerFactory,
		pkgdurable.DurableTypeFileArchive,
		cfg.Handler(),
	)
	return fsarchivehandler.NewFailureArchivePipeline(
		ctx,
		timeSource,
		cfg,
		tracer,
		meter,
		fsarchivehandler.InternalLogHandlers{
			OnError: handlers.OnError,
			OnWarn:  handlers.OnWarn,
			OnInfo:  handlers.OnInfo,
			OnDebug: handlers.OnDebug,
		},
		fsarchivehandler.WithCompressedLogSpoolRuntimes(opts.CompressedLogSpoolRuntimes),
	)
}

func buildInternalHandlers(
	factory InternalLogHandlerFactory,
	durableType pkgdurable.DurableType,
	cfg loggerhandler.ConfigProvider,
) InternalHandlers {
	if factory == nil || cfg == nil {
		return InternalHandlers{}
	}
	return factory(durableType, cfg)
}
