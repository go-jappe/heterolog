# heterolog

[English](README.md)

heterologは、ログを確実に届けたいGoのアプリケーションのためのログライブラリです。1回のログ呼出しで、標準出力・ファイル・Slack・OpenSearch・Loki・CloudWatch Logs・ClickHouseのうち複数の出力先へ同時に記録します。ログサーバーに届かないときは、ログをローカルのディスクに残し、サーバーが戻ったときに送り直します。アプリケーションを再起動した後も同じです。標準の `log/slog` からも利用できます。

- モジュールパス: `github.com/go-jappe/heterolog`
- Go 1.27.3以降が必要です
- ライセンス: [MIT License](LICENSE)

このREADMEは概要です。動作の詳細は[詳細の文書](docs/reference.ja.md)に、APIは [pkg.go.dev](https://pkg.go.dev/github.com/go-jappe/heterolog) にあります。このリポジトリのほかの文書（`.env.example` のコメントを含む）の多くは日本語で書かれています。

## 目次

- [heterologを使う理由](#heterologを使う理由)
- [特長](#特長)
- [インストール](#インストール)
- [クイックスタート](#クイックスタート)
- [複数の出力先へ送る](#複数の出力先へ送る)
- [ロガーの選び方](#ロガーの選び方)
- [出力先で失敗したとき](#出力先で失敗したとき)
- [稼働中にログレベルとサンプリングを変える](#稼働中にログレベルとサンプリングを変える)
- [ログファイル](#ログファイル)
- [トレース](#トレース)
- [メトリクス](#メトリクス)
- [終了処理](#終了処理)
- [停止したアプリケーションのログの復旧](#停止したアプリケーションのログの復旧)
- [テスト](#テスト)
- [設計の理由](#設計の理由)
- [ライセンス](#ライセンス)

## heterologを使う理由

よくある構成では、ログを標準出力やファイルに書き、その配送はFluent BitやVectorのような別のログ配送ツール（エージェントやサイドカー）に任せます。heterologは、代わりにアプリケーション自身からログを送ります。

- **アプリケーションが、それぞれのログがどうなったかを知れます。** 通常のAPIはログを記録・保存できなかった場合にerrorを返し、Delivery APIは出力先ごとの結果を返します。配送ツールを使う場合、アプリケーションが知れるのはローカルのファイルへ書いたことまでです。
- **送れなかったログを失いません。** ローカルのspoolに保存し、出力先が回復したら送り直します。`Retry-After` に従い、再起動後も引き継ぎます。これは、エージェントのディスクバッファが通常担う役割にあたります。結果が分からないログは、二重に送ったり削除したりせず、人が確かめられるように取り分けます。
- **ほかに配置するものがありません。** アプリケーションごとに、容量の見積もり・監視・更新が必要な別のプロセスを置かずに済みます。溢れたり止まったりし得る中間のファイルやソケットもありません。それ自身がエージェント・デーモン・コマンドラインツールであるGoのプログラムにも向いています。
- **再起動せずにログレベルを変えられます。** 例えば、調査の間だけHTTPの要求1回で出力先をDEBUGに切り替え、終わったら戻せます。

代わりに、配送にはアプリケーション自身のCPU・メモリ・ネットワークを使い、出力先の変更には新しい設定での再起動が必要です。次の場合は、ほかの道具のほうが合っているかもしれません。

- 標準出力に構造化ログを書くだけなら、`log/slog` だけ、またはzapやzerologのようなライブラリのほうが簡単です。
- 異なる言語で書かれた多くのプログラムが同じホストを共有する場合や、ルーティングを一か所でまとめて変える必要がある場合は、通常は専用のログ配送ツールのほうが適しています。

## 特長

- 出力先: stdout、file、Slack、OpenSearch、Loki、CloudWatch Logs、ClickHouse。自由に組み合わせられます。
- 保証の異なる3種類のロガー: 同期（errorを返す）、非同期（best effort）、Delivery（出力先ごとの結果を返す）。
- `log/slog` のハンドラー。
- ログファイルのローテーション、圧縮（gzip・zstd・lz4・xz）、保持、S3またはBackblaze B2などのS3互換のストレージへのアップロード。
- 送れなかったログを保存するローカルのspool。自動で送り直し、`Retry-After` には再起動後も従います。
- 空き容量の閾値、任意のメモリ逼迫の監視器、`drop`・`degrade` の方針による過負荷の保護。
- コードでの設定、または `HETEROLOG_` 環境変数と環境設定ファイルからの設定。
- 稼働中のログレベルとサンプリングの変更。コードまたはトークンで保護したHTTP APIから行います。
- トレース: spanを、OTLP/HTTPの受信先、OpenSearch（Trace Analytics用）、file、stdoutへ、まとめて再試行付きで送る `Tracer` と、OpenTelemetry SDK用のexporter。送れなかったspanは、ログと同じspoolとfailure archiveへ渡します。ロガーも自分の処理をspanとして記録できます。
- メトリクス: counter、gauge、histogramを、OTLP/HTTPの受信先、VictoriaMetrics、file、stdoutへ、同じ再試行、spool、failure archive付きで送る `Meter`。
- 停止中のアプリケーションのログファイルを復旧するCLI。
- Linux・Windows・macOSで、大文字小文字を区別しないファイルシステムも含めて試験しています。

## インストール

```sh
go get github.com/go-jappe/heterolog
```

## クイックスタート

次のプログラムは、標準出力へ2件のログを書きます。同様のプログラムが [examples/slog](examples/slog/main.go) にあり、このリポジトリのチェックアウト内で `go run ./examples/slog` で実行できます。

```go
package main

import (
    "context"
    "log"
    "log/slog"

    "github.com/go-jappe/heterolog"
)

func main() {
    ctx := context.Background()
    runtime, err := heterolog.New(ctx, heterolog.DefaultConfig())
    if err != nil {
        log.Fatal(err)
    }
    defer func() {
        if err := runtime.Close(); err != nil {
            log.Print(err)
        }
    }()

    logger := slog.New(heterolog.NewSlogHandler(runtime, nil))
    logger.InfoContext(ctx, "service started", "service", "example")
    logger.With("component", "worker").WithGroup("job").InfoContext(ctx,
        "job completed", "id", 42)
}
```

既定では、INFOのログを標準出力へ、WARNとERRORのログを標準エラーへ記録し、日時はUTCです。グローバルなロガーにする場合は `slog.SetDefault(logger)` を呼びます。`slog` と各出力先はそれぞれ別にレベルで絞り込むため、DEBUGのログを書くには両方のレベルを下げます（[slogの利用](docs/reference.ja.md#slogの利用)を参照）。

## 複数の出力先へ送る

いちばん手軽なのは、`LoadEnvConfig` で環境設定ファイルを読む方法です。次のファイルは、標準出力・ログファイル・Lokiへ記録します。

```sh
# logging.env
HETEROLOG_TARGET=my-service
HETEROLOG_LOG_TYPE=stdout,file,loki
HETEROLOG_LOKI_ENDPOINT=http://loki.example.internal:3100
```

```go
env, err := heterolog.LoadEnvConfig("logging.env") // ""なら環境変数だけを読む
if err != nil {
    log.Fatal(err)
}
runtime, err := env.New(ctx) // env.NewAsync・env.NewDeliveryも同じ
if err != nil {
    log.Fatal(err)
}
defer runtime.Close()

logger := slog.New(heterolog.NewSlogHandler(runtime, nil))
logger.InfoContext(ctx, "service started")
```

- `HETEROLOG_TARGET` は、ファイルやディレクトリの名前に使うアプリケーションの名前です。ログファイルの既定の保存先は、LinuxとBSDでは `$XDG_STATE_HOME`（なければ `~/.local/state`。rootで動く場合は `/var/log`）、macOSでは `~/Library/Logs`、Windowsでは `%LOCALAPPDATA%` の下です。この例では、そこに `my-service/logs/my-service_<日付>_1.log` を作り、ERRORとDEBUGのログは別のファイルに書きます。
- 省略した設定には既定値を使うため、ファイルには変えたい項目だけを書けば足ります。すべてのキーは [.env.example](.env.example) にあります（コメントは日本語です）。その値は設定の一例で、既定値と異なる項目があります。
- 何も設定しなければ、すべてのロガーは標準出力だけに書きます。ほかの出力先は選んだ場合だけ使い、足りない認証情報は補わずにエラーとして報告します。
- 設定は読み込み時に検証し、見つかった問題をまとめて返します。
- 環境変数に設定した値は、ファイルの同じキーより優先します。プロセスの環境変数は変更せず、ほかのファイルも読みません。

### 既定のログレベルとサンプリング

設定しなければ、すべての出力先がINFO以上を記録します。例外は通知に使うSlackで、ERROR以上を記録します。サンプリングはしないため、レベルを満たすログはすべて残ります。

| 出力先 | レベル | サンプリング |
| --- | --- | --- |
| Slack | ERROR | なし |
| stdout・file・OpenSearch・Loki・CloudWatch Logs・ClickHouse | INFO | なし |

- レベルのキーは `HETEROLOG_<出力先>_LOG_LEVEL`、サンプリングのキーは `HETEROLOG_ENABLE_LOG_<出力先>_SAMPLING` で、`<出力先>` は `STDOUT`・`FILE`・`SLACK`・`OPENSEARCH`・`LOKI`・`CLOUDWATCH`・`CLICKHOUSE` のいずれかです。非同期のロガーには、名前に `ASYNC_` を含む別のキーがあります。
- サンプリングを有効にすると、割合がNのレベルのログをN件に1件残します。request ID（`heterolog.WithRequestID`、またはslogの文字列の `request_id` 属性）を持つログは、代わりにrequest単位で残すかどうかを決められるため、残したrequestのログは欠けません。サンプリングを有効にするときは、割合も設定してください。省略した割合には既定値を使います（例えばOpenSearch・Loki・CloudWatch Logs・ClickHouseのINFOは1,000件に1件）。
- レベルとサンプリングは[稼働中にも変えられます](#稼働中にログレベルとサンプリングを変える)。
- Deliveryのロガーはサンプリングせず、レベルで絞り込むのはSlackの出力（ERROR）だけです。

### コードで設定する

`heterolog.DefaultConfig()` は、標準出力へ書く設定を返します。ほかの出力先は、`Config` の `File`・`Loki` などの項目を設定して加えます。すべての設定項目は [pkg/loggerの文書](https://pkg.go.dev/github.com/go-jappe/heterolog/pkg/logger) にあります。環境設定を元にコードで調整する場合は、`cfg, opts, err := env.Config()` で取り出し、`heterolog.New(ctx, cfg, opts...)` に渡します。

## ロガーの選び方

ロガーは3種類あり、ログ呼出しが呼び出し側に何を伝えるかが違います。

| 作成する関数 | ログ呼出しが返すもの | 向いている用途 |
| --- | --- | --- |
| `New` | `error` | 黙って失われてはならないログ |
| `NewAsync` | なし（送信を待ちません） | 大量のログ |
| `NewDelivery` | `DeliveryReport` と `error` | どこへ届いたかで次の処理を決めるログ |

`NewSlogHandler` は、`New` のロガーを `slog.Handler` にします。`slog.Logger` はハンドラーのerrorを捨てるため、errorが必要な場合はロガーを直接呼びます。

```go
jobID := heterolog.MustFieldKey("job_id")
if err := runtime.Info(ctx, "job completed", jobID.Int(42)); err != nil {
    // ログを記録・保存できなかった
}
```

Deliveryのreportは出力先ごとの結果を持ち、確定した成功と、失敗・送信前の拒否・結果不明・送信キューへの受理を区別します。

```go
delivery, err := heterolog.NewDelivery(ctx, heterolog.DefaultDeliveryConfig())
if err != nil {
    log.Fatal(err)
}
defer delivery.Close()

report, err := delivery.Info(ctx, "payment captured")
if err != nil || report.HasUnregisteredTarget() {
    // 確認できていない出力先がある。report.Targetsに出力先ごとの結果がある
}
```

通常のAPIの呼出しが成功しても、ログがbufferに受け付けられただけの場合があり、届いたことまでは保証しません。届いたことを報告するのはDeliveryのロガーだけです。

## 出力先で失敗したとき

OpenSearch・Loki・CloudWatch Logs・ClickHouseでは、送れなかったログを失敗の種類に応じて扱います。

| 起きたこと | heterologの処理 |
| --- | --- |
| タイムアウト・429・5xxなどの一時的な失敗 | spoolに保存し、後で送り直します。`Retry-After` に従い、再起動後も同じです |
| 送信後に接続が切れたなど、結果が分からない | `quarantine` へ移します。すでに届いている可能性があるため、自動では送り直しません |
| 出力先がログの内容を拒否した | バッチを分割して拒否されたログを見つけ、それだけを `quarantine` へ移します |
| 認証の拒否やtableの不在など、出力先や設定の誤り | `dead` へ移します |

- 保存したログは、既定では削除しません。それがログの唯一の写しである可能性があるためです。代わりに、ディスクを使い切らないように、空き容量が `HETEROLOG_MIN_AVAILABLE_DISK_SPACE_GB` を下回っている間は、新しい失敗を設定に従って破棄する（`drop`。`ErrLogDropped` として報告）か、標準出力へ書きます（`degrade`）。
- 二重に送るか失うかを選ぶ場合は、二重に送ります。
- 環境設定では、通常と非同期のロガーは既定でspoolを使います。Deliveryのロガーは送り直さず、送れなかったログをローカルのファイルに保存し、すべての結果を呼び出し側へ返します。
- Slackは送れなかったログを保存しません。S3へアップロードできなかったログファイルは残し、再びアップロードします。

規則の詳細は[詳細の文書](docs/reference.ja.md#失敗時の扱いと再送)にあります。

## 稼働中にログレベルとサンプリングを変える

通常と非同期のロガーの出力先ごとのレベルとサンプリングは、アプリケーションの稼働中に、再起動もロガーの作り直しもせずに変えられます。変更は次のログから反映し、メモリ上だけに保持します。再起動すると設定どおりに戻るため、一時的な変更がうっかり残ることはありません。

プロセスの外から変えるには、管理用API（admin API）を起動します。

```sh
# logging.env
# ホストを省略すると127.0.0.1だけで待ち受けます。
HETEROLOG_ADMIN_ADDR=:9100
# 32バイト以上。例えば openssl rand -hex 32 の出力
HETEROLOG_ADMIN_TOKEN=<トークン>
```

```go
if adminCfg, ok := env.AdminServerConfig(); ok {
    // 変更の要求をすべて記録します。既定では標準出力と、audit_logsのファイルです。
    audit, err := env.NewAdminAudit(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer audit.Close()
    admin, err := heterolog.StartAdminServer(adminCfg,
        map[string]heterolog.SettingsController{"app": runtime}, audit)
    if err != nil {
        log.Fatal(err)
    }
    defer admin.Close()
}
```

各ロガーに付けた名前（ここでは `app`）が、JSONのキーになります。

```sh
# すべてのロガーの設定を表示する
curl -H "Authorization: Bearer $HETEROLOG_ADMIN_TOKEN" http://127.0.0.1:9100/settings

# stdoutにDEBUGのログを書く
curl -X PATCH -H "Authorization: Bearer $HETEROLOG_ADMIN_TOKEN" \
  -d '{"loggers":{"app":{"outputs":{"stdout":{"level":"DEBUG"}}}}}' \
  http://127.0.0.1:9100/settings

# ログファイルにはINFOのログを10件に1件だけ残す
curl -X PATCH -H "Authorization: Bearer $HETEROLOG_ADMIN_TOKEN" \
  -d '{"loggers":{"app":{"outputs":{"file":{"sampling":{"enabled":true,"by_request_id":false,"debug_rate":1,"info_rate":10,"warn_rate":1,"error_rate":1}}}}}}' \
  http://127.0.0.1:9100/settings
```

どの要求も、すべてのロガーの設定を返します。例えば、2つ目の要求の後のstdoutの部分は次のとおりです（fileとlokiは省いています）。

```json
{
  "loggers": {
    "app": {
      "outputs": {
        "stdout": {
          "level": "DEBUG",
          "sampling": {
            "enabled": false,
            "by_request_id": true,
            "request_id_rate": 10,
            "debug_rate": 1,
            "info_rate": 1,
            "warn_rate": 1,
            "error_rate": 1
          }
        }
      }
    }
  }
}
```

- 割合がNなら、そのレベルのログをN件に1件残します。`by_request_id` を有効にすると、request IDを持つログはrequest単位で残すかどうかを決めます（`request_id_rate` 件のrequestに1件）。そのため、残したrequestのログは欠けません。
- レベルは `DEBUG`・`INFO`・`WARN`・`ERROR` です。`PATCH` で省略した項目は今の値のままです。`PATCH` はまとめて検証し、一つでも不正なら何も反映せず、理由を付けて `400` を返します。
- すべての要求でトークンが必要です。比較は一定時間で行い、エラーにトークンを含めません。127.0.0.1以外で待ち受ける場合は、ホストとTLSの証明書・鍵（`HETEROLOG_ADMIN_TLS_CERT_FILE`・`HETEROLOG_ADMIN_TLS_KEY_FILE`）を指定します。トークンを平文で流さないためです。
- 変更は1件ずつ順に処理し、監査ログに記録します。記録するのは、変更の要求、変更を始める時点の設定、変更後の設定と、拒否した要求です（記録しないのは、成功した `GET` だけです）。どの記録にも `log_kind=audit` のフィールドと要求IDが付き、要求IDは応答の `X-Request-Id` でも返します。
- 監査ログは専用の送達確認ロガーが書き、既定では標準出力と、ほかのログのディレクトリと並ぶ `audit_logs` のファイルに残します。CloudWatch Logsなどの出力先は `HETEROLOG_ADMIN_AUDIT_LOG_TYPE` で選べます。詳しくは[管理用APIの監査ログ](docs/reference.ja.md#管理用apiの監査ログ)を見てください。
- 変更後の設定が要求どおりでない場合、変更は戻しません。応答は `500` で今の設定を返し、食い違いをERRORで記録します。監査ログの出力先にSlackを加えると、これが通知になります。監査の記録が出力先のすべてに届かない場合も変更は続け、応答に警告を含めます。
- 自分のHTTPサーバーで提供する場合は、同じ監査用ロガーを渡して `NewAdminHandler` を使います。コードでは、`runtime.OutputSettings()` が設定を返し、`runtime.UpdateOutputSettings(changes)` が設定を変えます。
- 稼働中に変えられるのはレベルとサンプリングだけで、出力先や保存先は変えられません。Deliveryのロガーには、この設定はありません。

詳しくは[稼働中の設定変更](docs/reference.ja.md#稼働中の設定変更)を参照してください。

## ログファイル

- 日・週（UTCの日曜日0時から）・月ごとのローテーションと、ファイルが最大サイズに達したときの新しいファイルへの切り替え。
- ERRORとDEBUGのログを、それぞれ別のファイルに書けます（環境設定では既定で分けます）。
- gzip・zstd・lz4・xzでの圧縮。元のログを削除する前に、圧縮したファイルを元のログと照合します。
- 世代数・経過日数・ディレクトリのサイズによる保持。
- 圧縮したファイルのS3へのアップロード。アップロードの記録は再起動後も引き継ぎます。
- ファイル名は、ファイルシステムの大文字小文字の規則に合わせて照合します（Windows、macOS、Linuxの大文字小文字を区別しないディレクトリ）。

ローテーション・保持・圧縮・アップロードの詳細は[詳細の文書](docs/reference.ja.md#ログファイル)にあります。

## トレース

`NewTelemetry` は、spanを、OpenTelemetry CollectorやGrafana TempoなどのOTLP/HTTPの受信先、OpenSearch、file、stdoutへ送ります。その `Tracer` はアプリケーションのspanを記録し、`WithObservers` でロガーへ渡すと、ロガーの処理も子のspanとして記録します。

```go
cfg := heterolog.DefaultTelemetryConfig()
cfg.Resource.ServiceName = "checkout"
cfg.Traces.Exporters = []heterolog.TraceExporter{heterolog.TraceExporterOTLP}
cfg.Traces.OTLP.Endpoint = "http://localhost:4318/v1/traces"
tel, err := heterolog.NewTelemetry(ctx, cfg)
if err != nil {
    log.Fatal(err)
}
defer tel.Shutdown(context.Background())

runtime, err := heterolog.New(ctx, heterolog.DefaultConfig(),
    heterolog.WithObservers(tel.Tracer(), nil))
if err != nil {
    log.Fatal(err)
}
defer runtime.Close()

ctx, span := tel.Tracer().StartSpan(ctx, "checkout")
defer span.End()
```

- 環境変数からは、`HETEROLOG_ENABLE_TRACES=true` と `HETEROLOG_TRACES_OTLP_ENDPOINT` を設定し、`env.NewTelemetry(ctx)` を呼びます。サービス名は `HETEROLOG_APP_NAME` から取ります。
- spanは、出力先ごとの待ち行列からまとめて送るため、spanを終えるときにネットワークを待ちません。失敗した要求は送り直します。それでも受信先が受け付けなかったspanは、ログと同じspoolとfailure archiveに残せます。spoolのspanは、受信先が回復したら、再起動後も含めて送り直します。残さないspanと、待ち行列に入らなかったspanは捨て、メトリクスで数え、`WithTelemetryErrorHandler` で渡した関数（渡さなければ標準のlogger）に知らせます。
- `TraceExporterOpenSearch` は、spanを1件ずつ、OpenSearch Data Prepperと同じ形の文書として書くため、OpenSearch DashboardsのTrace Analyticsでspanを見られます。先に[index template](docs/opensearch/otel-v1-apm-span-index-template.json)を登録してください。
- `pkg/observer` はW3Cの `traceparent` headerを読み書きするため、traceをサービスをまたいで続けられます。
- OpenTelemetry SDKのspanも、`otelexport.NewSpanExporter(tel)` で同じように送れます。SDKを使わないプログラムがSDKをリンクしないよう、別のパッケージにしています。

詳しくは[トレースの送信](docs/reference.ja.md#トレースの送信)を参照してください。

## メトリクス

同じ `Telemetry` が、メトリクスを、OTLP/HTTPの受信先（OpenTelemetry Collector、Prometheus、VictoriaMetricsなど）、VictoriaMetricsのPrometheus形式の取り込みAPI、file、stdoutへ送ります。その `Meter` は、counter、gauge、histogramを記録し、`WithObservers` でロガーへ渡せば、ロガーの `heterolog_*` のメトリクスも記録します。

```go
cfg.Metrics.Exporters = []heterolog.MetricExporter{heterolog.MetricExporterOTLP}
cfg.Metrics.OTLP.Endpoint = "http://localhost:4318/v1/metrics"
tel, err := heterolog.NewTelemetry(ctx, cfg)

tel.Meter().Counter("orders_total").Add(ctx, 1, heterolog.ObserverAttr{Key: "region", Value: "eu"})
```

- 環境変数からは、`HETEROLOG_ENABLE_METRICS=true` と `HETEROLOG_METRICS_OTLP_ENDPOINT` を設定します。
- 値は `ExportInterval`（既定は1分）ごとに集め、累積値（cumulative）として送ります。そのため、捨てたり失ったりした回があっても、次の回に合計が残ります。
- OpenTelemetry SDKのメトリクスも、SDKのreaderへ `otelexport.NewMetricExporter(tel)` を渡せば、同じように送れます。
- 送れなかったメトリクスは、ログやspanと同じspoolとfailure archiveへ渡します。結果の分からない送信は、失うより送り直すため、同じ値が2回届くことがあります。累積値なので、2回目は同じ時刻の同じ値です。

詳しくは[メトリクスの送信](docs/reference.ja.md#メトリクスの送信)を参照してください。

## 終了処理

- アプリケーションの終了前に `Close` または `Shutdown(ctx)` を呼びます。受け付けた処理を終え、bufferを書き出し、資源を解放します。`Telemetry` は、`Shutdown(ctx)` で待ち行列のspanを送り、メトリクスを最後にもう一度集めて送ります。その `Tracer` と `Meter` を使うロガーを先に閉じてください。
- `Shutdown` のcontextが終わると、待つのをやめるのはその呼出しだけで、後始末は続きます。`ForceClose` は後始末を止めるよう求め、待たずに戻ります。
- spoolに残ったログは、次の起動の後に送ります。

詳しくは[終了処理](docs/reference.ja.md#終了処理)を参照してください。

## 停止したアプリケーションのログの復旧

停止したアプリケーションのログファイルを、アプリケーションと同じ環境設定で復旧するCLIも同梱しています。

```sh
go build -o log-recovery ./cmd/log-recovery
./log-recovery --target my-service --application-stopped --env-file /path/to/logging.env
```

検証の内容と指定方法は[ログ復旧CLI](docs/reference.ja.md#ログ復旧cli)を、何を復旧するかと運用の手順は[手動でのログ復旧](docs/manual-log-recovery.txt)を参照してください。

## テスト

```sh
go test ./...
go vet ./...
```

通常のテストに外部サービスは要りません。CIでは、WindowsとmacOSでも全体を試験し、Linuxでは大文字小文字を区別しない一時ディレクトリと、シンボリックリンクを経由する一時ディレクトリでも試験します。さらに、コンテナで動かすOpenSearch・Loki・ClickHouse・Tempo・OpenTelemetry Collectorとの結合テストも行います。raceの試験と実サービスとの結合テストは[テスト](docs/reference.ja.md#テスト)を参照してください。

## 設計の理由

### 3種類のロガーと、失敗したログを残すこと

ログによって、呼び出し側が知る必要のあることが違います。そのため、オプション付きの一つのロガーではなく、3種類のロガーを用意しています。

- `New` は同期で、ログを書けなかった・保存できなかった場合にerrorを返します。黙って失われてはならないログに使います。
- `NewAsync` はbest effortで、呼び出し側に配送を待たせず、呼出しごとのerrorも返しません。大量のログに使います。
- `NewDelivery` は、確定した成功・失敗・送信前の拒否・結果不明・キューへの受理を区別した、出力先ごとのreportを返します。ログがどこへ届いたかで次の処理を決める呼び出し側に使います。
- それぞれの型は、保証の内容をメソッドの形で表します（同期ロガーのログ呼出しはerrorを返し、非同期ロガーは何も返さず、Deliveryロガーはreportを返します）。そのため、コードを読めば、どの保証に頼っているかが分かります。

送れなかったログは、捨てずに残します。

- 一時的な失敗はspoolへ保存して再送します。`Retry-After` に従い、その期限は再起動後も引き継ぐため、回復中の出力先へ一度に送りつけることはありません。
- 送信後に接続が切れた場合など、結果が分からないときは、自動では再送せず `quarantine` へ移します。再送すれば二重に届くおそれがあり、削除すれば失われるおそれがあるため、人が判断します。
- 内容のために拒否されたログは、バッチを分割して一件ずつ切り分け、一件の不正なログが他を止めないようにします。認証の拒否やtableの不在など、出力先や設定が原因の失敗は `dead` へ移します。
- 重複と欠落のどちらかを選ぶ場合は、重複を選びます。

残した失敗は、既定では削除しません。それがログの唯一の写しである可能性があるため、削除は明示的な判断にするべきで、保持の上限は利用者が有効にする設定にしています。ディスクを使い切ることは、代わりに空き容量の閾値（`HETEROLOG_MIN_AVAILABLE_DISK_SPACE_GB`）で防ぎます。閾値を下回ると、新しい失敗は設定に従って破棄（`ErrLogDropped` として報告）されるか、標準出力へ退避されます。

### 設定

- 設定は明示的に渡します。パッケージのimportやロガーの構築には副作用がありません。envファイルの読込、シグナルハンドラーの登録、グローバルなslogロガーやOpenTelemetry providerの置き換えを行いません。ライブラリは、それを使うアプリケーションの状態を変えるべきではないためです。
- 環境変数を読むのは、利用者が選んだ場合だけです。`LoadEnvConfig` は呼ばれたときだけ、渡されたファイルだけを読み、プロセスの環境変数を変更しません。多くのdotenvツールと同じく、環境変数に設定した値がファイルより優先されます。
- すべてのキーが `HETEROLOG_` で始まるため、アプリケーション自身の設定と同じ環境変数や `.env` ファイルに置いても衝突しません。
- 何も設定しなければ、すべてのロガーは標準出力だけに書きます。そのため、設定も外部サービスもなしで動きます。ほかの出力先は選んだ場合だけ有効になり、足りない認証情報は補わずにエラーとして報告します。
- サンプリングは、利用者が選んだ場合だけ行います。既定では、すべての出力先がINFO以上のログをすべて記録します（通知に使うSlackはERROR以上）。そのため、サンプリングを有効にしない限り、レベルを満たすログを捨てることはありません。
- 設定は読み込み時またはロガーの構築時に検証し、すべての問題をまとめて報告します。そのため、誤った設定は最初のログや最初の失敗のときではなく、起動時に失敗します。
- 稼働中に変えられるのは、ログレベルとサンプリングだけです。出力先はこれらをログごとに読むため、変更にロガーの作り直しも記録の中断も要りません。出力先や保存先の変更には、それが必要になります。HTTP APIはトークンが必要で、TLSを設定しない限り127.0.0.1だけで待ち受け、変更は再起動後に残りません。そのため、調査中にDEBUGへ切り替えるような一時的な変更が、うっかり残り続けることはありません。
- 設定エラーには、認証情報を含み得る値（HTTP header、S3の宛先の一覧、URLのuser infoなど）を含めません。
- 既定値は環境に合わせています。日時はUTCです。Linux・BSD・illumos・Solaris・AIXでrootで動かない場合、保存先は利用者のstate directory（`$XDG_STATE_HOME` が絶対パスならそこ、そうでなければ `~/.local/state`）にするため、一般利用者も追加の設定なしでファイル出力を有効にできます。

### 実際のシステムでの試験

- ログを失ったり重複させたりする可能性が最も高い部分は、OSとファイルシステムに依存します。名前の大文字小文字の区別、置換しないrename、ディレクトリの同期、ファイルの権限とWindowsのACL、一時ディレクトリのシンボリックリンク、macOSのAppleDoubleファイルです。入力を差し替えるだけではこれらの振る舞いを再現できないため、単体テストに加えて、実際のファイルシステムで全体を試験します。
- CIでは、Linux・Windows・macOSで全体を試験します。さらにLinuxでは、シンボリックリンクを経由する一時ディレクトリと、大文字小文字を区別しない一時ディレクトリ（ext4のcasefoldとvfat）での試験と、コンテナで動かすOpenSearch・Loki・ClickHouse・Tempo・OpenTelemetry Collectorとの結合テストを行います。
- こうした条件を確かめるためのjobでは、試験がskipされると失敗にします。そのため、確認の範囲が知らないうちに失われることはありません。
- CIとは別に、Windowsの仮想マシン上で一般ユーザーと管理者のそれぞれで、またmacOSの実機上で大文字小文字を区別するAPFS・ExFAT・FAT32のdisk imageを使って、全体を試験しています。結果は [testenv](testenv) に記録しています。
- 時間に依存する振る舞いは、可能な場合は仮想時間（`testing/synctest`）で試験します。

## ライセンス

[MIT License](LICENSE)
