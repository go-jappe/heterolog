# VictoriaMetrics Push Manager

VictoriaMetrics Push Managerは、システムメトリクスとカスタムメトリクスを定期的にVictoriaMetricsに送信するためのコンポーネントです。

## 機能

- **自動メトリクス収集**: CPU、メモリ、Goランタイムのメトリクスを自動的に収集
- **定期的なプッシュ**: 設定された間隔でメトリクスを自動送信
- **カスタムメトリクス**: アプリケーション固有のメトリクスを追加可能
- **バッチ処理**: 大量のメトリクスを効率的に送信
- **非同期送信**: ブロッキングを避けるための非同期送信オプション
- **エラーハンドリング**: リトライ機能とカスタムエラーハンドラー
- **ヘルスチェック**: VictoriaMetricsエンドポイントの可用性確認

## 使用方法

`PushInterval`、`BatchSize`、`WorkerCount`、`Timeout` の0は、それぞれ30秒、1000、2、10秒で補います。
負数の間隔・容量・ワーカー数・タイムアウト・再試行設定は構築時に拒否します。
`MaxRetries: 0` は再試行なし、`RetryDelay: 0` は再試行の待機なしを意味します。
`Stop()` は `Start()` 前でも受理済みの非同期送信を完了させ、ワーカーを閉じます。

### 基本的な使用例

```go
package main

import (
    "context"
    "log"
    "time"
    
    "your-module/internal/infrastructure/victoriametrics"
)

func main() {
    // 設定を作成
    config := &victoriametrics.PushManagerConfig{
        Endpoint:     "http://localhost:8428/api/v1/import/prometheus",
        PushInterval: 30 * time.Second,
        AppName:      "my-app",
        Environment:  "production",
        InstanceID:   "instance-1",
    }
    
    // PushManagerを作成
    pm, err := victoriametrics.NewPushManager(config)
    if err != nil {
        log.Fatal(err)
    }
    defer pm.Stop()
    
    // 自動プッシュを開始
    if err := pm.Start(); err != nil {
        log.Fatal(err)
    }
    
    // アプリケーションのメインロジック
    // ...
}
```

### カスタムメトリクスの追加

```go
// 単一のメトリクスを追加
pm.AddCustomMetric("http_requests_total", 1000, map[string]string{
    "method": "GET",
    "path":   "/api/users",
    "status": "200",
})

// 複数のメトリクスを一度に設定
metrics := []vm.Metric{
    {
        Name:  "cache_hits_total",
        Value: 500,
        Labels: map[string]string{
            "cache": "redis",
        },
    },
    {
        Name:  "cache_misses_total",
        Value: 50,
        Labels: map[string]string{
            "cache": "redis",
        },
    },
}
pm.SetCustomMetrics(metrics)
```

### 即座にプッシュ

```go
// 同期的にプッシュ
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := pm.PushNow(ctx); err != nil {
    log.Printf("Push failed: %v", err)
}

// 非同期でプッシュ
if err := pm.PushAsync(); err != nil {
    log.Printf("Async push failed: %v", err)
}
```

### エラーハンドリング

```go
config := &victoriametrics.PushManagerConfig{
    Endpoint: "http://localhost:8428/api/v1/import/prometheus",
    OnError: func(err error) {
        // カスタムエラーハンドリング
        log.Printf("VictoriaMetrics error: %v", err)
        // アラート送信、メトリクス記録など
    },
}
```

### 高度な設定

```go
config := &victoriametrics.PushManagerConfig{
    Endpoint:     "http://localhost:8428/api/v1/import/prometheus",
    PushInterval: 1 * time.Minute,
    AppName:      "my-app",
    Environment:  "production",
    InstanceID:   "instance-1",
    
    // カスタムラベル（すべてのメトリクスに追加される）
    CustomLabels: map[string]string{
        "region":     "us-east-1",
        "datacenter": "dc1",
        "team":       "backend",
    },
    
    // パフォーマンス設定
    BatchSize:    5000,        // バッチサイズ
    WorkerCount:  4,           // 非同期ワーカー数
    Timeout:      30 * time.Second,
    
    // リトライ設定
    MaxRetries: 5,
    RetryDelay: 2 * time.Second,
}
```

## 収集されるシステムメトリクス

### CPU メトリクス
- `system_cpu_usage_percent`: CPU使用率（パーセント）

### メモリメトリクス
- `system_memory_used_bytes`: 使用中のメモリ（バイト）
- `system_memory_total_bytes`: 総メモリ容量（バイト）
- `system_memory_usage_percent`: メモリ使用率（パーセント）

### Goランタイムメトリクス
- `go_goroutines`: 現在のゴルーチン数
- `go_alloc_bytes`: 割り当てられたメモリ（バイト）

### 内部統計メトリクス
- `vm_push_manager_push_total`: 総プッシュ回数
- `vm_push_manager_error_total`: 総エラー回数
- `vm_push_manager_last_push_timestamp`: 最後のプッシュのタイムスタンプ

## ベストプラクティス

1. **適切な間隔の設定**: メトリクスの重要度とシステムの負荷を考慮して、プッシュ間隔を設定してください。

2. **エラーハンドリング**: 本番環境では必ずカスタムエラーハンドラーを設定し、エラーを適切に記録・通知してください。

3. **グレースフルシャットダウン**: アプリケーション終了時は必ず`Stop()`を呼び出して、残りのメトリクスを送信してください。

4. **ラベルの使用**: メトリクスを効果的にフィルタリング・集計できるよう、適切なラベルを設定してください。

5. **バッチサイズの調整**: ネットワーク帯域幅とレイテンシを考慮して、適切なバッチサイズを設定してください。

## トラブルシューティング

### メトリクスが送信されない

1. VictoriaMetricsエンドポイントが正しいか確認
2. ネットワーク接続を確認
3. `HealthCheck`メソッドでエンドポイントの可用性を確認

```go
ctx := context.Background()
if err := pm.HealthCheck(ctx); err != nil {
    log.Printf("Health check failed: %v", err)
}
```

### メモリ使用量が増加する

1. バッチサイズを小さくする
2. プッシュ間隔を短くする
3. カスタムメトリクスを定期的にクリアする

```go
// 不要になったカスタムメトリクスをクリア
pm.ClearCustomMetrics()
```

### 統計情報の確認

```go
stats := pm.GetStats()
log.Printf("Running: %v", stats["running"])
log.Printf("Push count: %d", stats["push_count"])
log.Printf("Error count: %d", stats["error_count"])
log.Printf("Last push: %v", stats["last_push"])
```

## 依存関係

- `github.com/shirou/gopsutil/v3`: システムメトリクスの収集
- 内部パッケージ:
  - `internal/infrastructure/metrics`: システムメトリクスコレクター
  - `pkg/victoriametrics`: VictoriaMetricsクライアントとフォーマッター
