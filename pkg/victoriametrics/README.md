# VictoriaMetrics Client Package

このパッケージは、VictoriaMetricsにメトリクスを送信するためのGoクライアントライブラリです。Prometheusフォーマットのメトリクスをサポートし、同期・非同期送信、バッチ処理、リトライ機能などを提供します。

## 主な機能

- **同期・非同期送信**: メトリクスを即座に送信、またはバックグラウンドで効率的に送信
- **バッチ処理**: 大量のメトリクスを自動的にバッチに分割して送信
- **自動リトライ**: ネットワークエラーや一時的な障害に対する自動リトライ
- **ヘルスチェック**: VictoriaMetricsエンドポイントの可用性確認
- **カスタマイズ可能**: タイムアウト、リトライ、バッチサイズなどの設定が可能
- **エラーハンドリング**: 非同期送信時のエラーを処理するカスタムハンドラー
- **Prometheusフォーマット**: 標準的なPrometheusテキスト形式をサポート

## インストール

```bash
go get github.com/go-jappe/heterolog/pkg/victoriametrics
```

## 使用方法

### 基本的な使用例

```go
import (
    "context"
    "log"
    "github.com/go-jappe/heterolog/pkg/victoriametrics"
)

func main() {
    // クライアントを作成
    client := victoriametrics.NewClient("http://localhost:8428/api/v1/import/prometheus")
    defer client.Close()

    // メトリクスを定義
    metrics := []victoriametrics.Metric{
        {
            Name: "http_requests_total",
            Labels: map[string]string{
                "method": "GET",
                "status": "200",
            },
            Value: 1234,
        },
        {
            Name:  "cpu_usage_percent",
            Value: 85.5,
        },
    }

    // 同期送信
    ctx := context.Background()
    if err := client.Send(ctx, metrics); err != nil {
        log.Printf("Failed to send metrics: %v", err)
    }
}
```

### カスタム設定

```go
client := victoriametrics.NewClient("http://localhost:8428/api/v1/import/prometheus",
    // タイムアウトを設定
    victoriametrics.WithTimeout(30*time.Second),
    // リトライ設定
    victoriametrics.WithRetry(5, 2*time.Second),
    // バッチサイズを設定
    victoriametrics.WithBatchSize(500),
    // ワーカー数を設定
    victoriametrics.WithWorkerCount(8),
    // エラーハンドラーを設定
    victoriametrics.WithErrorHandler(func(err error) {
        log.Printf("Metrics error: %v", err)
    }),
)
defer client.Close()
```

### 非同期送信

```go
// エラーハンドラーを定義
errorHandler := func(err error) {
    log.Printf("Async send error: %v", err)
}

client := victoriametrics.NewClient("http://localhost:8428/api/v1/import/prometheus",
    victoriametrics.WithErrorHandler(errorHandler),
    victoriametrics.WithBatchSize(100),
    victoriametrics.WithWorkerCount(4),
)
defer client.Close()

// 非同期でメトリクスを送信
for i := 0; i < 1000; i++ {
    metrics := []victoriametrics.Metric{
        {
            Name: "async_counter",
            Labels: map[string]string{
                "job": "example",
            },
            Value: float64(i),
        },
    }

    if err := client.SendAsync(metrics); err != nil {
        log.Printf("Failed to queue metrics: %v", err)
    }
}
```

`SendAsync` はメトリクスのスライスと各 `Labels` をコピーして受理します。
呼び出しが戻った後は、元のスライスやラベルマップを変更・再利用できます。
呼び出し中は入力を変更しないでください。

### ヘルスチェック

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := client.HealthCheck(ctx); err != nil {
    log.Printf("VictoriaMetrics is not healthy: %v", err)
} else {
    log.Println("VictoriaMetrics is healthy")
}
```

## 設定オプション

| オプション | 説明 | デフォルト値 |
|-----------|------|-------------|
| `WithTimeout` | HTTPリクエストのタイムアウト | 10秒 |
| `WithRetry` | 初回の送信に加える再試行の回数と待機時間 | 3回、1秒 |
| `WithBatchSize` | バッチあたりの最大メトリクス数 | 1000 |
| `WithWorkerCount` | 非同期ワーカーの数 | 2 |
| `WithErrorHandler` | エラーハンドラー関数 | なし |
| `WithFormatter` | カスタムメトリクスフォーマッター | PrometheusTextFormatter |
| `WithHTTPClient` | カスタムHTTPクライアント | デフォルトクライアント |

`WithHTTPClient` の設定はコピーし、`Transport`、`Jar`、`CheckRedirect` を引き継ぎます。
元の HTTP クライアントは変更しません。タイムアウトには各 VictoriaMetrics クライアントの
`WithTimeout`（未指定なら10秒）を使います。オプションの指定順にかかわらず、
元の HTTP クライアントの `Timeout` より優先します。

`WithTimeout` は同期・非同期ともに各HTTP要求へ適用し、再送のたびに同じ長さの期限を新しく設けます。
バッチ分割や再送待機を含む送信全体には、この値から別の期限を設けません。
`WithTimeout(0)` はHTTPの期限を設けず、`Close` は受理済み送信の完了を待ちます。

`WithRetry` は通信エラーと一時的なHTTPエラー（5xx・429・408）だけを再送します。
回数は初回の送信に加える再試行の回数で、1なら最大2回、0なら再試行せずに1回だけ送ります。
n回目の再試行の前には、待機時間のn倍だけ待ちます。負の回数では、`Send` と `SendAsync` が
`ErrInvalidRetryCount` を返します。
メトリクスの検証・変換やHTTP要求の生成に失敗した場合は、送信や待機をせずにすぐエラーを返します。

メトリクス送信では、POSTのメソッドや本文を失うリダイレクトを停止し、元のHTTPエラーを返します。
307・308によるPOSTと本文を保った転送は、HTTPクライアントのリダイレクト設定に従います。

`WithWorkerCount` が0以下の場合、非同期ワーカーは起動しません。
`SendAsync` はデータを受理せずエラーを返します。同期送信の `Send` は利用できます。

`WithBatchSize(0)` は既定値の1000を使用します。負数を指定した場合はワーカーを起動せず、
`Send` と `SendAsync` が送信前にエラーを返します。

## メトリクスフォーマット

### Metric構造体

```go
type Metric struct {
    Name   string            // メトリクス名
    Labels map[string]string // ラベル(key-value)
    Value  float64           // 値
}
```

標準のフォーマッターはタイムスタンプを出力しないため、時刻はVictoriaMetricsが受信時に記録します。

### バリデーション

メトリクスはPrometheusの命名規則に従う必要があります：

```go
metric := victoriametrics.Metric{
    Name: "valid_metric_name",
    Labels: map[string]string{
        "env": "production",
    },
    Value: 42,
}

if err := metric.Validate(); err != nil {
    log.Printf("Invalid metric: %v", err)
}
```

### カスタムフォーマッター

独自のフォーマッターを実装することも可能です：

```go
type customFormatter struct{}

func (f *customFormatter) Format(metrics []victoriametrics.Metric) (string, error) {
    // カスタムフォーマット実装
    var result string
    for _, m := range metrics {
        result += fmt.Sprintf("CUSTOM: %s = %f\n", m.Name, m.Value)
    }
    return result, nil
}

client := victoriametrics.NewClient("http://localhost:8428",
    victoriametrics.WithFormatter(&customFormatter{}),
)
```

## エラーハンドリング

### HTTPエラー

HTTPエラーは`HTTPError`型として返され、リトライ可能かどうかを判定できます。`Send` はバッチの範囲やリトライ回数を付けてエラーをラップするため、`errors.As` で取り出します：

```go
if err := client.Send(ctx, metrics); err != nil {
    var httpErr *victoriametrics.HTTPError
    if errors.As(err, &httpErr) {
        log.Printf("HTTP error: %d %s", httpErr.StatusCode, httpErr.Body)
        if httpErr.IsRetryable() {
            // リトライ可能なエラー
        }
    }
}
```

Close後の送信、0以下のbatch size・worker数は、それぞれ `ErrClientClosed`、`ErrInvalidBatchSize`、`ErrInvalidWorkerCount` を `errors.Is` で判定できます。

### 非同期エラー

非同期送信時のエラーはエラーハンドラーで処理されます：

```go
errorCh := make(chan error, 100)

client := victoriametrics.NewClient("http://localhost:8428",
    victoriametrics.WithErrorHandler(func(err error) {
        select {
        case errorCh <- err:
        default:
            log.Printf("Error channel full: %v", err)
        }
    }),
)

// 別のゴルーチンでエラーを処理
go func() {
    for err := range errorCh {
        log.Printf("Async error: %v", err)
    }
}()
```

## パフォーマンス

### バッチ処理

大量のメトリクスは自動的にバッチに分割されます：

```go
client := victoriametrics.NewClient("http://localhost:8428",
    victoriametrics.WithBatchSize(1000),
)

// 10,000個のメトリクスが10バッチに分割される
metrics := make([]victoriametrics.Metric, 10000)
// ... メトリクスを生成 ...

if err := client.Send(ctx, metrics); err != nil {
    log.Printf("Failed to send batch: %v", err)
}
```

### 非同期バッファリング

非同期送信では、メトリクスがバッファリングされ、効率的に送信されます：

- バッファがいっぱいになったとき
- 一定時間が経過したとき
- クライアントが閉じられたとき

## テスト

```bash
# ユニットテストの実行
go test -v ./pkg/victoriametrics

# ベンチマークテストの実行
go test -bench=. -benchmem ./pkg/victoriametrics

# カバレッジの確認
go test -cover ./pkg/victoriametrics
```

## ベストプラクティス

1. **クライアントの再利用**: クライアントは再利用可能です。アプリケーション全体で1つのインスタンスを使用してください。

2. **適切なクローズ**: `defer client.Close()`を使用して、非同期ワーカーが適切に終了することを確認してください。

3. **エラーハンドリング**: 非同期送信を使用する場合は、必ずエラーハンドラーを設定してください。

4. **バッチサイズの調整**: ネットワーク帯域幅とVictoriaMetricsの処理能力に応じて、適切なバッチサイズを設定してください。

5. **ヘルスチェック**: 定期的にヘルスチェックを実行して、VictoriaMetricsの可用性を確認してください。

## トラブルシューティング

### "async channel is full"エラー

このエラーは `errors.Is(err, victoriametrics.ErrAsyncChannelFull)` で判定できます。非同期チャネルがいっぱいの場合、以下の対策を検討してください：

- ワーカー数を増やす: `WithWorkerCount(8)`
- チャネルサイズを増やす（コード変更が必要）
- メトリクス生成レートを下げる

### リトライが多い場合

- VictoriaMetricsの負荷を確認
- ネットワーク接続を確認
- タイムアウト値を増やす: `WithTimeout(30*time.Second)`

### メモリ使用量が多い場合

- バッチサイズを小さくする: `WithBatchSize(100)`
- 非同期送信の代わりに同期送信を使用

## ライセンス

このパッケージはプロジェクトのライセンスに従います。
