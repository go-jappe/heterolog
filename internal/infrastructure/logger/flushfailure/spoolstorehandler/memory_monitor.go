package spoolstorehandler

import (
	"context"
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	memorymonitor "github.com/go-jappe/heterolog/internal/observer/memory/pressure"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// MemoryPressurePollIntervalは、spoolが自ら起動したmemory逼迫の監視で、OSのmemoryを読む間隔です。
const MemoryPressurePollInterval = time.Second

// StartMemoryPressureMonitorは、memory逼迫の監視を渡されなかったspoolのために、spoolの設定の
// MemoryCriticalRatio・MinAvailableMemoryBytes・MemoryCriticalConsecutiveHitsで判定する監視を
// providerから作り、起動します。UseMemoryPressureForOverloadが無効なら何も作らず、nilを返します。
// 戻り値のstopは監視を止める関数で、handlerのStopMemoryMonitorOnShutdownへ渡します。
// 監視は、読むたびにmemoryの使用率と判定の結果を、backendの名前でmeterのgaugeへ記録します。
// 閾値が不正な場合や最初の読み取りに失敗した場合はerrorを返し、監視を残しません。
func StartMemoryPressureMonitor(
	cfg OverloadConfigProvider,
	provider memorypressure.Provider,
	meter pkgobserver.Meter,
	backend string,
) (memorypressure.Monitor, func(), error) {
	if cfg == nil {
		return nil, nil, nil
	}
	overloadCfg := cfg.OverloadPolicyConfig()
	if overloadCfg == nil || !overloadCfg.UseMemoryPressureForOverload() {
		return nil, nil, nil
	}
	monitor, err := memorymonitor.NewObserved(provider, memorypressure.CriticalPolicy{
		CriticalRatio:           overloadCfg.MemoryCriticalRatio(),
		MinAvailableBytes:       overloadCfg.MinAvailableMemoryBytes(),
		ConsecutiveCriticalHits: overloadCfg.MemoryCriticalConsecutiveHits(),
	}, MemoryPressurePollInterval, func(ctx context.Context, pressure memorypressure.MemoryPressure, critical bool) {
		telemetry.RecordMemoryPressure(ctx, meter, backend, pressure, critical)
	})
	if err != nil {
		return nil, nil, fmt.Errorf("create spool memory pressure monitor: %w", err)
	}
	// 監視はhandlerのShutdownが完了するまで動かします。構築時のcontextが終わっても止めません。
	ctx, stop := context.WithCancel(context.Background())
	if err := monitor.Start(ctx); err != nil {
		stop()
		return nil, nil, fmt.Errorf("start spool memory pressure monitor: %w", err)
	}
	return monitor, stop, nil
}

// StopMemoryMonitorOnShutdownは、このhandlerのために起動したmemory逼迫の監視を、Shutdownの
// 完了時に止めるようにします。失敗の受付を始める前の構築中にだけ呼びます。stopがnilなら何もしません。
func (s *SpoolStoreHandler) StopMemoryMonitorOnShutdown(stop func()) {
	if s == nil {
		return
	}
	s.operationMu.Lock()
	s.stopMemoryMonitor = stop
	s.operationMu.Unlock()
}
