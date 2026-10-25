package victoriametrics

import (
	"context"
	"testing"
	"time"

	vm "github.com/go-jappe/heterolog/pkg/victoriametrics"
)

func TestNewPushManager(t *testing.T) {
	tests := []struct {
		name    string
		config  *PushManagerConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: &PushManagerConfig{
				Endpoint:     "http://localhost:8428/api/v1/import/prometheus",
				PushInterval: 30 * time.Second,
				AppName:      "test-app",
				Environment:  "test",
			},
			wantErr: false,
		},
		{
			name:    "nil config uses defaults",
			config:  nil,
			wantErr: true, // エンドポイントが必須
		},
		{
			name: "missing endpoint",
			config: &PushManagerConfig{
				AppName: "test-app",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, err := NewPushManager(tt.config)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewPushManager() error = nil, wantErr %v", tt.wantErr)
				}
				if pm != nil {
					t.Errorf("NewPushManager() pm = %v, want nil", pm)
				}
			} else {
				if err != nil {
					t.Errorf("NewPushManager() error = %v, wantErr %v", err, tt.wantErr)
				}
				if pm == nil {
					t.Errorf("NewPushManager() pm = nil, want non-nil")
				}
				if pm != nil {
					pm.Stop()
				}
			}
		})
	}
}

func TestPushManager_AddCustomMetric(t *testing.T) {
	config := &PushManagerConfig{
		Endpoint:    "http://localhost:8428/api/v1/import/prometheus",
		AppName:     "test-app",
		Environment: "test",
		InstanceID:  "test-instance",
	}

	pm, err := NewPushManager(config)
	if err != nil {
		t.Fatalf("NewPushManager() error = %v", err)
	}
	defer pm.Stop()

	// カスタムメトリクスを追加
	pm.AddCustomMetric("custom_metric", 42.0, map[string]string{
		"custom_label": "value",
	})

	// メトリクスが追加されたことを確認
	if len(pm.customMetrics) != 1 {
		t.Errorf("customMetrics length = %d, want 1", len(pm.customMetrics))
	}
	if pm.customMetrics[0].Name != "custom_metric" {
		t.Errorf("metric name = %s, want custom_metric", pm.customMetrics[0].Name)
	}
	if pm.customMetrics[0].Value != 42.0 {
		t.Errorf("metric value = %f, want 42.0", pm.customMetrics[0].Value)
	}
	if pm.customMetrics[0].Labels["custom_label"] != "value" {
		t.Errorf("custom_label = %s, want value", pm.customMetrics[0].Labels["custom_label"])
	}
	if pm.customMetrics[0].Labels["app"] != "test-app" {
		t.Errorf("app label = %s, want test-app", pm.customMetrics[0].Labels["app"])
	}
	if pm.customMetrics[0].Labels["env"] != "test" {
		t.Errorf("env label = %s, want test", pm.customMetrics[0].Labels["env"])
	}
}

func TestPushManager_SetCustomMetrics(t *testing.T) {
	config := &PushManagerConfig{
		Endpoint:    "http://localhost:8428/api/v1/import/prometheus",
		AppName:     "test-app",
		Environment: "test",
	}

	pm, err := NewPushManager(config)
	if err != nil {
		t.Fatalf("NewPushManager() error = %v", err)
	}
	defer pm.Stop()

	// 複数のカスタムメトリクスを設定
	metrics := []vm.Metric{
		{
			Name:  "metric1",
			Value: 10.0,
			Labels: map[string]string{
				"label1": "value1",
			},
		},
		{
			Name:  "metric2",
			Value: 20.0,
			Labels: map[string]string{
				"label2": "value2",
			},
		},
	}

	pm.SetCustomMetrics(metrics)

	// メトリクスが設定されたことを確認
	if len(pm.customMetrics) != 2 {
		t.Errorf("customMetrics length = %d, want 2", len(pm.customMetrics))
	}
	if pm.customMetrics[0].Name != "metric1" {
		t.Errorf("first metric name = %s, want metric1", pm.customMetrics[0].Name)
	}
	if pm.customMetrics[1].Name != "metric2" {
		t.Errorf("second metric name = %s, want metric2", pm.customMetrics[1].Name)
	}
}

func TestPushManager_ClearCustomMetrics(t *testing.T) {
	config := &PushManagerConfig{
		Endpoint:    "http://localhost:8428/api/v1/import/prometheus",
		AppName:     "test-app",
		Environment: "test",
	}

	pm, err := NewPushManager(config)
	if err != nil {
		t.Fatalf("NewPushManager() error = %v", err)
	}
	defer pm.Stop()

	// カスタムメトリクスを追加
	pm.AddCustomMetric("custom_metric", 42.0, nil)
	if len(pm.customMetrics) != 1 {
		t.Errorf("customMetrics length after add = %d, want 1", len(pm.customMetrics))
	}

	// クリア
	pm.ClearCustomMetrics()
	if len(pm.customMetrics) != 0 {
		t.Errorf("customMetrics length after clear = %d, want 0", len(pm.customMetrics))
	}
}

func TestPushManager_GetStats(t *testing.T) {
	config := &PushManagerConfig{
		Endpoint:    "http://localhost:8428/api/v1/import/prometheus",
		AppName:     "test-app",
		Environment: "test",
	}

	pm, err := NewPushManager(config)
	if err != nil {
		t.Fatalf("NewPushManager() error = %v", err)
	}
	defer pm.Stop()

	stats := pm.GetStats()
	if stats["running"].(bool) != false {
		t.Errorf("running = %v, want false", stats["running"])
	}
	if stats["push_count"].(uint64) != 0 {
		t.Errorf("push_count = %d, want 0", stats["push_count"])
	}
	if stats["error_count"].(uint64) != 0 {
		t.Errorf("error_count = %d, want 0", stats["error_count"])
	}
}

func TestPushManager_parseMetricLine(t *testing.T) {
	config := &PushManagerConfig{
		Endpoint:    "http://localhost:8428/api/v1/import/prometheus",
		AppName:     "test-app",
		Environment: "test",
		InstanceID:  "test-instance",
	}

	pm, err := NewPushManager(config)
	if err != nil {
		t.Fatalf("NewPushManager() error = %v", err)
	}
	defer pm.Stop()

	tests := []struct {
		name    string
		line    string
		want    vm.Metric
		wantErr bool
	}{
		{
			name: "valid metric line",
			line: "cpu_usage_percent 75.5",
			want: vm.Metric{
				Name:  "cpu_usage_percent",
				Value: 75.5,
				Labels: map[string]string{
					"app":      "test-app",
					"env":      "test",
					"instance": "test-instance",
				},
			},
			wantErr: false,
		},
		{
			name:    "invalid metric line - no value",
			line:    "cpu_usage_percent",
			wantErr: true,
		},
		{
			name:    "invalid metric line - invalid value",
			line:    "cpu_usage_percent invalid",
			wantErr: true,
		},
		{
			name:    "empty line",
			line:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := pm.parseMetricLine(tt.line)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseMetricLine() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if got.Name != tt.want.Name {
					t.Errorf("metric name = %s, want %s", got.Name, tt.want.Name)
				}
				if got.Value != tt.want.Value {
					t.Errorf("metric value = %f, want %f", got.Value, tt.want.Value)
				}
				// ラベルの比較
				for k, v := range tt.want.Labels {
					if got.Labels[k] != v {
						t.Errorf("label %s = %s, want %s", k, got.Labels[k], v)
					}
				}
			}
		})
	}
}

func TestPushManager_mergeLabels(t *testing.T) {
	config := &PushManagerConfig{
		Endpoint:    "http://localhost:8428/api/v1/import/prometheus",
		AppName:     "test-app",
		Environment: "test",
		InstanceID:  "test-instance",
		CustomLabels: map[string]string{
			"region": "us-east-1",
			"team":   "backend",
		},
	}

	pm, err := NewPushManager(config)
	if err != nil {
		t.Fatalf("NewPushManager() error = %v", err)
	}
	defer pm.Stop()

	inputLabels := map[string]string{
		"method": "GET",
		"status": "200",
		"app":    "override-app", // デフォルトを上書き
	}

	merged := pm.mergeLabels(inputLabels)

	// デフォルトラベルが含まれていることを確認
	if merged["app"] != "override-app" {
		t.Errorf("app label = %s, want override-app", merged["app"])
	}
	if merged["env"] != "test" {
		t.Errorf("env label = %s, want test", merged["env"])
	}
	if merged["instance"] != "test-instance" {
		t.Errorf("instance label = %s, want test-instance", merged["instance"])
	}

	// カスタムラベルが含まれていることを確認
	if merged["region"] != "us-east-1" {
		t.Errorf("region label = %s, want us-east-1", merged["region"])
	}
	if merged["team"] != "backend" {
		t.Errorf("team label = %s, want backend", merged["team"])
	}

	// 入力ラベルが含まれていることを確認
	if merged["method"] != "GET" {
		t.Errorf("method label = %s, want GET", merged["method"])
	}
	if merged["status"] != "200" {
		t.Errorf("status label = %s, want 200", merged["status"])
	}
}

// 使用例を示すExample関数
func ExamplePushManager() {
	// 設定を作成
	config := &PushManagerConfig{
		Endpoint:     "http://localhost:8428/api/v1/import/prometheus",
		PushInterval: 30 * time.Second,
		AppName:      "my-app",
		Environment:  "production",
		InstanceID:   "instance-1",
		CustomLabels: map[string]string{
			"region": "us-east-1",
			"team":   "backend",
		},
		OnError: func(err error) {
			// エラーログを記録
			println("Error:", err.Error())
		},
	}

	// PushManagerを作成
	pm, err := NewPushManager(config)
	if err != nil {
		panic(err)
	}
	defer pm.Stop()

	// 自動的な定期プッシュを開始
	if err := pm.Start(); err != nil {
		panic(err)
	}

	// カスタムメトリクスを追加
	pm.AddCustomMetric("http_requests_total", 1000, map[string]string{
		"method": "GET",
		"path":   "/api/users",
		"status": "200",
	})

	// 即座にプッシュ
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pm.PushNow(ctx); err != nil {
		println("Push failed:", err.Error())
	}

	// 非同期プッシュ
	if err := pm.PushAsync(); err != nil {
		println("Async push failed:", err.Error())
	}

	// 統計情報を取得
	stats := pm.GetStats()
	println("Push count:", stats["push_count"])
}
