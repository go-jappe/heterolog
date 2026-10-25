// Package victoriametrics provides functionality for pushing metrics to VictoriaMetrics.
package victoriametrics

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/metrics"

	vm "github.com/go-jappe/heterolog/pkg/victoriametrics"
)

// PushManagerConfig はPushManagerの設定
type PushManagerConfig struct {
	// VictoriaMetricsのエンドポイント
	Endpoint string

	// メトリクス収集間隔。0は30秒、負数は無効。
	PushInterval time.Duration

	// アプリケーション名（メトリクスのラベルとして使用）
	AppName string

	// 環境名（メトリクスのラベルとして使用）
	Environment string

	// インスタンスID（メトリクスのラベルとして使用）
	InstanceID string

	// カスタムラベル
	CustomLabels map[string]string

	// バッチサイズ。0は1000、負数は無効。
	BatchSize int

	// ワーカー数。0は2、負数は無効。
	WorkerCount int

	// タイムアウト。0は10秒、負数は無効。
	Timeout time.Duration

	// リトライ設定
	MaxRetries int
	RetryDelay time.Duration

	// エラーハンドラー
	OnError func(error)
}

// DefaultConfig はデフォルト設定を返す
func DefaultConfig() *PushManagerConfig {
	return &PushManagerConfig{
		PushInterval: 30 * time.Second,
		AppName:      "heterolog",
		Environment:  "production",
		InstanceID:   "default",
		BatchSize:    1000,
		WorkerCount:  2,
		Timeout:      10 * time.Second,
		MaxRetries:   3,
		RetryDelay:   time.Second,
		OnError: func(err error) {
			log.Printf("VictoriaMetrics push error: %v", err)
		},
	}
}

// PushManager はVictoriaMetricsへのメトリクスプッシュを管理する
type PushManager struct {
	config    *PushManagerConfig
	client    vm.MetricsClient
	collector metrics.SystemMetricsCollector

	// cancelはStartした周期処理をStopから中止するために保持します。
	// Context自体はpushLoopの引数で渡します。
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex

	// 状態
	running    bool
	stopped    bool
	lastPush   time.Time
	pushCount  uint64
	errorCount uint64

	// カスタムメトリクス
	customMetrics []vm.Metric
	customMu      sync.RWMutex
}

// NewPushManager は新しいPushManagerを作成する
func NewPushManager(config *PushManagerConfig) (*PushManager, error) {
	effective, err := normalizePushManagerConfig(config)
	if err != nil {
		return nil, err
	}
	config = effective

	// VictoriaMetricsクライアントを作成
	clientOpts := []vm.ClientOption{
		vm.WithTimeout(config.Timeout),
		vm.WithRetry(config.MaxRetries, config.RetryDelay),
		vm.WithBatchSize(config.BatchSize),
		vm.WithWorkerCount(config.WorkerCount),
		vm.WithErrorHandler(config.OnError),
	}

	client := vm.NewClient(config.Endpoint, clientOpts...)

	// システムメトリクスコレクターを作成
	collector := metrics.NewSystemMetricsCollector()

	return &PushManager{
		config:    config,
		client:    client,
		collector: collector,
	}, nil
}

// Start はメトリクスの定期的なプッシュを開始する。
// Stopは送信clientも閉じるため、停止後に再開する場合は新しいPushManagerを作成する。
func (pm *PushManager) Start() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.running {
		return fmt.Errorf("push manager is already running")
	}
	if pm.stopped {
		return fmt.Errorf("push manager cannot be restarted after Stop")
	}

	ctx, cancel := context.WithCancel(context.Background())
	pm.cancel = cancel
	pm.running = true
	pm.wg.Add(1)
	go pm.pushLoop(ctx)

	return nil
}

// Stopは次の周期処理を停止し、開始済みの送信・エラー通知と最後の送信を待ってからclientを閉じます。
// 開始済みの送信はStopで中止せず、その送信に設定したtimeoutまで継続します。
// Start前でも非同期ワーカーを閉じ、受理済みのPushAsyncを処理し終えてから返します。
func (pm *PushManager) Stop() error {
	pm.mu.Lock()
	if pm.stopped {
		pm.mu.Unlock()
		return fmt.Errorf("push manager is not running")
	}
	pm.stopped = true
	pm.running = false
	cancel := pm.cancel
	pm.mu.Unlock()

	// キャンセルシグナルを送信
	if cancel != nil {
		cancel()
	}

	// ワーカーの終了を待つ
	pm.wg.Wait()

	// クライアントをクローズ
	return pm.client.Close()
}

// PushNow は即座にメトリクスをプッシュする
func (pm *PushManager) PushNow(ctx context.Context) error {
	metrics, err := pm.collectAllMetrics()
	if err != nil {
		return fmt.Errorf("failed to collect metrics: %w", err)
	}

	if err := pm.client.Send(ctx, metrics); err != nil {
		pm.incrementErrorCount()
		return fmt.Errorf("failed to send metrics: %w", err)
	}

	pm.updateLastPush()
	pm.incrementPushCount()
	return nil
}

// PushAsync は非同期でメトリクスをプッシュする
func (pm *PushManager) PushAsync() error {
	metrics, err := pm.collectAllMetrics()
	if err != nil {
		return fmt.Errorf("failed to collect metrics: %w", err)
	}

	return pm.client.SendAsync(metrics)
}

// AddCustomMetric はカスタムメトリクスを追加する
func (pm *PushManager) AddCustomMetric(name string, value float64, labels map[string]string) {
	pm.customMu.Lock()
	defer pm.customMu.Unlock()

	metric := vm.Metric{
		Name:   name,
		Value:  value,
		Labels: pm.mergeLabels(labels),
	}

	pm.customMetrics = append(pm.customMetrics, metric)
}

// SetCustomMetrics はカスタムメトリクスを設定する（既存のものを置き換える）
func (pm *PushManager) SetCustomMetrics(metrics []vm.Metric) {
	pm.customMu.Lock()
	defer pm.customMu.Unlock()

	// ラベルをマージ
	for i := range metrics {
		metrics[i].Labels = pm.mergeLabels(metrics[i].Labels)
	}

	pm.customMetrics = metrics
}

// ClearCustomMetrics はカスタムメトリクスをクリアする
func (pm *PushManager) ClearCustomMetrics() {
	pm.customMu.Lock()
	defer pm.customMu.Unlock()

	pm.customMetrics = nil
}

// GetStats は統計情報を返す
func (pm *PushManager) GetStats() map[string]interface{} {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return map[string]interface{}{
		"running":     pm.running,
		"last_push":   pm.lastPush,
		"push_count":  pm.pushCount,
		"error_count": pm.errorCount,
	}
}

// HealthCheck はVictoriaMetricsエンドポイントのヘルスチェックを行う
func (pm *PushManager) HealthCheck(ctx context.Context) error {
	return pm.client.HealthCheck(ctx)
}

// pushLoop は定期的にメトリクスをプッシュする
func (pm *PushManager) pushLoop(ctx context.Context) {
	defer pm.wg.Done()

	ticker := time.NewTicker(pm.config.PushInterval)
	defer ticker.Stop()

	// 周期の停止要求と、開始済み送信の期限を分けます。Stopでctxが終了しても、
	// 受理した送信とそのエラー通知が完了するまではclientを閉じません。
	workCtx := context.WithoutCancel(ctx)
	pushCtx, cancel := context.WithTimeout(workCtx, pm.config.Timeout)
	if err := pm.PushNow(pushCtx); err != nil {
		pm.config.OnError(fmt.Errorf("initial push failed: %w", err))
	}
	cancel()

	for {
		// 実行中の送信から戻った時点で停止要求があれば、溜まったtickは処理しません。
		if ctx.Err() != nil {
			break
		}
		select {
		case <-ticker.C:
			if ctx.Err() != nil {
				break
			}
			pushCtx, cancel := context.WithTimeout(workCtx, pm.config.Timeout)
			if err := pm.PushNow(pushCtx); err != nil {
				pm.config.OnError(fmt.Errorf("periodic push failed: %w", err))
			}
			cancel()

		case <-ctx.Done():
			// 停止後の最終送信は、周期処理を抜けて一度だけ実行します。
		}
	}
	pushCtx, cancel = context.WithTimeout(workCtx, pm.config.Timeout)
	if err := pm.PushNow(pushCtx); err != nil {
		pm.config.OnError(fmt.Errorf("final push failed: %w", err))
	}
	cancel()
}

// collectAllMetrics はすべてのメトリクスを収集する
func (pm *PushManager) collectAllMetrics() ([]vm.Metric, error) {
	var allMetrics []vm.Metric

	// システムメトリクスを収集
	systemMetrics, err := pm.collectSystemMetrics()
	if err != nil {
		return nil, err
	}
	allMetrics = append(allMetrics, systemMetrics...)

	// カスタムメトリクスを追加
	pm.customMu.RLock()
	customMetrics := make([]vm.Metric, len(pm.customMetrics))
	copy(customMetrics, pm.customMetrics)
	pm.customMu.RUnlock()

	allMetrics = append(allMetrics, customMetrics...)

	// 内部統計メトリクスを追加
	statsMetrics := pm.collectStatsMetrics()
	allMetrics = append(allMetrics, statsMetrics...)

	return allMetrics, nil
}

// collectSystemMetrics はシステムメトリクスを収集する
func (pm *PushManager) collectSystemMetrics() ([]vm.Metric, error) {
	var metrics []vm.Metric

	// CPU メトリクス
	cpuMetrics, err := pm.collector.CollectCPUMetrics()
	if err != nil {
		return nil, fmt.Errorf("failed to collect CPU metrics: %w", err)
	}
	for _, m := range cpuMetrics {
		metric, err := pm.parseMetricLine(m)
		if err != nil {
			continue
		}
		metrics = append(metrics, metric)
	}

	// メモリメトリクス
	memMetrics, err := pm.collector.CollectMemoryMetrics()
	if err != nil {
		return nil, fmt.Errorf("failed to collect memory metrics: %w", err)
	}
	for _, m := range memMetrics {
		metric, err := pm.parseMetricLine(m)
		if err != nil {
			continue
		}
		metrics = append(metrics, metric)
	}

	// ランタイムメトリクス
	runtimeMetrics := pm.collector.CollectRuntimeMetrics()
	for _, m := range runtimeMetrics {
		metric, err := pm.parseMetricLine(m)
		if err != nil {
			continue
		}
		metrics = append(metrics, metric)
	}

	return metrics, nil
}

// collectStatsMetrics は内部統計メトリクスを収集する
func (pm *PushManager) collectStatsMetrics() []vm.Metric {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return []vm.Metric{
		{
			Name:   "vm_push_manager_push_total",
			Value:  float64(pm.pushCount),
			Labels: pm.getDefaultLabels(),
		},
		{
			Name:   "vm_push_manager_error_total",
			Value:  float64(pm.errorCount),
			Labels: pm.getDefaultLabels(),
		},
		{
			Name:   "vm_push_manager_last_push_timestamp",
			Value:  float64(pm.lastPush.Unix()),
			Labels: pm.getDefaultLabels(),
		},
	}
}

// parseMetricLine はメトリクス行をパースする
func (pm *PushManager) parseMetricLine(line string) (vm.Metric, error) {
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return vm.Metric{}, fmt.Errorf("invalid metric line: %s", line)
	}

	name := parts[0]
	value := parts[1]

	var floatValue float64
	if _, err := fmt.Sscanf(value, "%f", &floatValue); err != nil {
		return vm.Metric{}, fmt.Errorf("failed to parse value: %w", err)
	}

	return vm.Metric{
		Name:   name,
		Value:  floatValue,
		Labels: pm.getDefaultLabels(),
	}, nil
}

// getDefaultLabels はデフォルトのラベルを返す
func (pm *PushManager) getDefaultLabels() map[string]string {
	labels := make(map[string]string)

	labels["app"] = pm.config.AppName
	labels["env"] = pm.config.Environment
	labels["instance"] = pm.config.InstanceID

	// カスタムラベルを追加
	for k, v := range pm.config.CustomLabels {
		labels[k] = v
	}

	return labels
}

// mergeLabels はラベルをマージする
func (pm *PushManager) mergeLabels(labels map[string]string) map[string]string {
	merged := pm.getDefaultLabels()

	for k, v := range labels {
		merged[k] = v
	}

	return merged
}

// updateLastPush は最後のプッシュ時刻を更新する
func (pm *PushManager) updateLastPush() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.lastPush = time.Now()
}

// incrementPushCount はプッシュカウントをインクリメントする
func (pm *PushManager) incrementPushCount() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.pushCount++
}

// incrementErrorCount はエラーカウントをインクリメントする
func (pm *PushManager) incrementErrorCount() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.errorCount++
}
