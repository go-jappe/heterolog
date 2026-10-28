package spoolstorehandler

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/overload"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/overload/provider"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	spoolstoreconfig "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/store"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// OverloadConfigProviderはspoolの過負荷判定と滞留警告に使う設定を提供します。
type OverloadConfigProvider interface {
	OverloadPolicyConfig() spoolstoreconfig.SpoolStoreOverloadPolicyConfigProvider
}

func NewSpoolStoreHandler(
	ctx context.Context,
	timeSource clock.Clock,
	store *store.SpoolStore,
	cfg OverloadConfigProvider,
	memoryMonitor memorypressure.Monitor,
	overloadHandler overload.Handler,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	internalLogHandlers InternalLogHandlers,
) *SpoolStoreHandler {
	handler := &SpoolStoreHandler{
		clock:           timeSource,
		Store:           store,
		Config:          cfg,
		MemoryMonitor:   memoryMonitor,
		OverloadHandler: overloadHandler,
		Tracer:          tracer,
		Meter:           meter,
		dirStatsScanner: scanDirStats,
		OnError:         internalLogHandlers.OnError,
		OnWarn:          internalLogHandlers.OnWarn,
		OnInfo:          internalLogHandlers.OnInfo,
		OnDebug:         internalLogHandlers.OnDebug,
	}
	// 保存先を塞いでいた項目の保全結果も、handlerで選択済みの警告通知先へ渡します。
	// 運用中にStoreの通知先を設定し直すことはありません。
	if store != nil {
		store.OnWarn = func(ctx context.Context, entry *logging.LogEntry, tracer pkgobserver.Tracer, meter pkgobserver.Meter) {
			if handler.OnWarn != nil {
				handler.OnWarn(ctx, entry, tracer, meter)
			}
		}
	}
	return handler
}

func NewOverloadHandlerProvider(
	cfg OverloadConfigProvider,
	meter pkgobserver.Meter,
) (overload.Handler, error) {
	if cfg == nil {
		return nil, errors.New("spool store config is nil")
	}
	overloadCfg := cfg.OverloadPolicyConfig()
	if overloadCfg == nil {
		return nil, errors.New("spool overload policy config is nil")
	}
	if err := durable.ValidateSpoolActiveBacklogWarnAfterDays(overloadCfg.ActiveBacklogWarnAfterDays()); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidSpoolOverloadPolicyConfig, err)
	}

	switch overloadCfg.OverloadPolicy() {
	case "":
		// env loaderはdropを明示する。直接providerを組み立てる既存利用では
		// zero valueを従来どおりdropとして扱う。
		return provider.NewOverloadDropHandler(meter), nil
	case durable.OverloadPolicyDrop:
		return provider.NewOverloadDropHandler(meter), nil
	case durable.OverloadPolicyDegrade:
		handler, err := provider.NewDegradeHandler(overloadCfg)
		if err != nil {
			return nil, err
		}
		return telemetry.CountFailureRecords(handler, meter, telemetry.ActionDegraded), nil
	default:
		return nil, fmt.Errorf(
			"%w: %q",
			ErrUnsupportedSpoolOverloadPolicy,
			overloadCfg.OverloadPolicy(),
		)
	}
}
