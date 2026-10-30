# heterologの詳細

[English](reference.md)

この文書では、heterologの動作を詳しく説明します。各APIが返すもの、設定の検証、ログファイルと失敗したログの扱い、出力先ごとの動作です。概要・インストール・例は [README](../README.ja.md) を、APIそのものは [pkg.go.dev](https://pkg.go.dev/github.com/go-jappe/heterolog) を参照してください。

## 目次

- [ログの書き込み](#ログの書き込み)
- [ロガーと終了処理](#ロガーと終了処理)
- [設定](#設定)
- [ログファイル](#ログファイル)
- [失敗時の扱いと再送](#失敗時の扱いと再送)
- [メトリクスとトレース](#メトリクスとトレース)
- [出力先ごとの注意](#出力先ごとの注意)
- [ログ復旧CLI](#ログ復旧cli)
- [テスト](#テスト)

## ログの書き込み

### ログ呼出しのエラー

通常のAPIからはログ処理のエラーを受け取れます。

```go
jobID := heterolog.MustFieldKey("job_id")
err := runtime.Info(ctx, "job completed", jobID.Int(42))
```

### フィールドの値

- `Any(key, value)` は生成時に値をJSONとして保持します。
- JSON化できない値や、配列・objectの入れ子が `MaxFieldNestingDepth`（9000段）を超える値は、生成時に `ErrFieldValueNotJSON` を返します。この上限は、spoolや各出力先の形式が外側に加える階層を含めても保存・再送できる深さです。
- slogの属性にも同じ上限を適用します。超えた記録は出力せず、`Handle` がエラーを返します。
- `ErrFieldValueNotJSON` のerrorは、JSON化できなかった理由（`MarshalJSON` が返したerrorなど）を表示文にだけ含みます。その理由は記録しようとした値を保持し得るため、`errors.Is`・`errors.As` で取り出せません。

### メタデータのキー

- `request_id`、`trace_id`、`span_id` は、値が文字列の場合にログのメタデータとして扱います。
- 数値・真偽値・オブジェクトなどは型を保って、通常の構造化フィールドへ保存します。
- 同じキーを複数回指定した場合は最後の値だけを使い、その値の型で分類します。
- これらの規則は `slog` のトップレベル属性にも適用します。

### スタックトレース

- `ErrorWithStack(ctx, message, err, fields...)` は、呼び出し時のアプリケーションのスタックを記録します。
- 非同期処理でも呼び出し元を保持し、各出力先の `ErrorTraceDepth` に従って表示します。深度0では呼び出し位置だけ、深度Nではそこから親Nフレームまでを表示します。デバッグモードでは `ErrorTraceDepthDebugMode` を使います。
- `err` に `nil` を渡した場合も拒否せず、エラー情報のないERRORログとしてスタックとともに記録します。

### slogの利用

- `slog.Logger` は `Handler.Handle` の戻り値を呼出側へ返しません。呼出時のエラーを確認するには通常のAPI、宛先別の送達状況を確認するには `NewDelivery` を使います。
- 通常APIの成功はバッファへの受理を表す場合があり、最終的な配送成功を保証しません。
- `slog` と各バックエンドにそれぞれレベルフィルターがあります。DEBUGを出力する場合は両方を設定します。

```go
cfg := heterolog.DefaultConfig()
cfg.Stdout.RuntimeConfig.LogLevel = heterolog.LevelDebug
runtime, err := heterolog.New(ctx, cfg)
// errを確認してから使用する
logger := slog.New(heterolog.NewSlogHandler(runtime, &slog.HandlerOptions{
    Level: slog.LevelDebug,
    AddSource: true,
}))
```

- `WithAttrs`、`WithGroup`、`LogValuer`、`LevelVar`、`ReplaceAttr` に対応しています。
- slog属性はJSON相当の値に正規化して構造化フィールドへ保存します。同じキーが重複する場合は後の値を採用します。
- `ReplaceAttr` によるメッセージの置換・削除はバックエンドの本文にも反映します。削除した本文は空文字、文字列以外へ変換した本文はJSON表現になります。配送レベルと発生時刻は元Recordの値を保ちます。

## ロガーと終了処理

### API

| API | 用途 |
| --- | --- |
| `DefaultConfig` / `New` | 同期API。処理結果をerrorとして返す（過負荷によるdropは `ErrLogDropped`） |
| `DefaultAsyncConfig` / `NewAsync` | 非同期のbest effort記録 |
| `DefaultDeliveryConfig` / `NewDelivery` | 宛先ごとの送達結果を返す（ClickHouseはキューへの受理を `DeliveryEnqueued` として返す） |
| `NewSlogHandler` | 同期Loggerを `slog.Handler` として利用 |
| `DefaultSpoolConfig` / `WithSpool` | 永続化した失敗ログの再送ワーカー設定 |
| `Close` / `Shutdown(ctx)` | 受理済み処理の終了・バッファflush・リソース解放 |
| `ForceClose` | ログの受付を止め、待機中の `Close`・`Shutdown` を `ErrForcedClose` で解放し、後処理・復旧処理へ停止を要求（完了は待たない） |

### 送達結果（Delivery report）

reportの宛先ごとの結果は、次のいずれかです。

| 結果 | 意味 |
| --- | --- |
| `DeliverySucceeded` | 送信先での成功を確認できた |
| `DeliveryFailed` | 送信先が明示的に拒否した |
| `DeliveryRejected` | 送信前の入力検証などで拒否した |
| `DeliveryUnconfirmed` | 応答の欠落・解析失敗などで成否を確認できない |
| `DeliveryEnqueued` | 送信キューへの受理だけを報告する |

- ClickHouseはバッチ挿入を前提とするため、Delivery APIでも送信完了を待たず、キューへ受理した時点で `DeliveryEnqueued` を返します。実際の送信は件数・バイト数・`FlushInterval` の到達時または `Close` で行い、その成否は返したreportへ反映されません。
- `HasTargetFailure()` は失敗確定・送信前拒否だけを数えます。成功未確認も含めて停止判断する場合は `HasUnregisteredTarget()` を使います。`DeliveryEnqueued` は成功未確認として扱うため、ClickHouseを宛先に含むreportの `HasUnregisteredTarget()` は `true` になります。
- 退避先への保存成功は、送信先での成功とは独立しています。
- 送信先の結果が一件もないreportは、`Error()` が `ErrNoDeliveryTargets` を返すのと同じく、`HasTargetFailure()`・`HasAnyFailure()` も `true` です。出力先が一つもない設定は構築時に拒否するため、このreportは内部の不整合でのみ生じます。
- 宛先別の結果の `Backend` は、送信先が返した結果か、loggerが代わりに作った結果（handlerのpanic・期限切れ、`Close` 開始後や一件の上限超過による送信前の拒否）かにかかわらず、`pkg/logger.LogOutputDestination` の名前（`stdout`・`file`・`slack`・`opensearch`・`loki`・`cloudwatchlogs`・`clickhouse`）です。
- 配送ハンドラーのpanicを回復した場合も、配送結果が返らなかった宛先は `DeliveryUnconfirmed` です。他の宛先の確定済み結果は保持します。
- CloudWatch Logsの送信エラーは、戻り値のerrorと宛先別の `Err` に元の原因を保持し、`errors.Is`・`errors.As` で判別できます。

### エラーと破棄されたログ

- OpenSearch・Loki・ClickHouse・CloudWatch Logsの通常APIで、送信に失敗したログを退避できなかった場合（退避先が未設定の場合を含む）、戻り値のerrorには退避処理のエラーに加えて送信先の元の原因も含まれ、`errors.Is`・`errors.As` で判別できます。ClickHouseでは、そのflushを実行したログ呼出しまたは `Close` が返します。
- 送信に失敗したログを、保存先（spool・failure archive）の過負荷によりoverload policyの `drop`（既定）で破棄した場合、通常APIは `heterolog.ErrLogDropped` を包んだerrorを返します。`errors.Is(err, heterolog.ErrLogDropped)` で判定できます。送信先の元の原因も同じerrorに含まれます。
- バッファへ受理済みのログを後のflushで破棄した場合は、そのflushを実行したログ呼出しまたは `Close` が返します。
- `degrade` で退避できた場合はerrorを返しません。
- 非同期APIのログ呼出しはerrorを返しません。`Close` の最終flushで破棄した場合は `Close` が返します。
- Delivery APIでは `DeliveryReport.HasDrop()` で確認します。
- 破棄件数は、メトリクス `heterolog_failure_records_total` の `action="dropped"` でも観測できます。

### 終了処理

- アプリケーション終了時は必ず `Close` または `Shutdown` を呼びます。
- `Shutdown` のcontextが終了すると、その呼出しの待機だけを終了し、後処理は継続します。再度 `Close` / `Shutdown` を呼ぶと終了を待てます。
- spool内の未送信データは次回の再送用に残る場合があります。独立した `Flush` APIはありません。
- ログファイルの圧縮・アップロードの失敗が続く間、`Close` はその処理が完了するまで戻りません。終了までの時間に上限が必要な場合は、`Shutdown(ctx)` の期限で待機を打ち切り、必要に応じて `ForceClose` で後処理を停止してください。
- `Close`／`Shutdown` は、受理済みの送信と、その送信で発生した内部ログの通知（`OnErrorConfig` などで選んだ通知先への書き込み）が終わるまで待ちます。件数・バイト数の閾値によってログ呼出しの中で始まったflushも含みます。
- 出力先の終了処理でpanicを回復した場合も、他の出力先の終了処理を続け、未完了の終了処理を再試行します。終了済みの出力先を再度閉じることはありません。

## 設定

### 設定の型

- `Config`、`AsyncConfig`、`DeliveryConfig` は `pkg/logger` の公開設定型です。出力先にはstdout、file、Slack、OpenSearch、Loki、CloudWatch Logs、ClickHouseを指定できます。各設定の詳細は `go doc github.com/go-jappe/heterolog/pkg/logger` で確認できます。
- spoolを使う同期・非同期の構成では `WithSpool` も渡してください。
- 通常出力が一つもない設定は、`New`・`NewAsync`・`NewDelivery` が構築前に、`pkg/logger` の `ErrNoActiveLogOutput` を含むエラー（`errors.Is` で判定可能）で拒否します。
- `LogOutputDestinations` が空の場合は、non-nilのbackend設定がすべて通常出力になります。内部ログ専用の保存先やclient設定だけでは出力先になりません。

### 構築時のエラー

構築時の設定エラーは `errors.Is`・`errors.As` で判別できます。

- nilのcontext: `ErrNilContext`。
- 0以下の `Timeout` と、`NewAsync` の0以下の `MaxParallel`: `pkg/logger.ConfigFieldError`（`Field` は `heterolog.ConfigFieldTimeout`・`heterolog.ConfigFieldMaxParallel`、種類は `ErrNonPositiveSetting`）。
- `NewDelivery` へ渡した `WithSpool`: `ErrDeliverySpoolUnsupported`。
- `WithCloudWatchLogsStreamRouting` の未対応の値: `ErrInvalidStreamRouting`（`*InvalidStreamRoutingError`）。
- 終了済みのcontext: そのerror（`context.Canceled` など）。
- 内部ログの通知先（`OnErrorConfig` など）に選んだ出力先の設定から、通知専用の書き込み先を作れない場合（不正な `TimeZone`、空の `LogLineDateFormat` など）: `ErrInvalidNotificationTarget`（`*NotificationTargetError`。`Source` はloggerの種類、`Backend` は通知先、`Err` は原因）。
- 表示設定やbackend設定値（buffer・送信上限・retry設定など）の不正: `pkg/logger.ConfigFieldError`（`Source`・`Backend`・`Field`。値を伴う検証では `Value`・`Limit`）。種類は `ErrNonPositiveSetting` などのsentinelで、`errors.Is` で判定できます。
- client設定の不正: `Field` が `ConfigFieldClient`、種類が `ErrInvalidClientConfig` です。理由は各backend package（例: `pkg/loki.ErrEndpointRequired`）のsentinelとして `errors.Is` で判別できます。
- endpointをURLとして解析できない場合: エラーの文面とerror treeの `*url.Error` には、URLのuser info（ユーザー名・パスワード）を `xxxxx` に置き換えたURLだけを含めます。
- 解決できないhandler selector: `pkg/logger.HandlerSelectorError`。
- Slackの `Intervals` の不正な要素: `errors.As` で取り出せる `pkg/logger.SlackIntervalError` の `Index` で位置を特定できます。
- fileへの書き込み時の空き容量不足への方針: 方針が `drop`・`degrade` 以外なら `pkg/fsstore.ErrUnsupportedOverloadPolicy`、`degrade` の退避先が `stdout` 以外なら `pkg/fsstore.ErrUnsupportedSpillDestination` です（`errors.Is` で判別できます）。

### 副作用とcontext

- importや `New` は、`.env` 読込、シグナル登録、サーバーの起動、グローバルロガーやOpenTelemetry providerの変更を行いません。設定はコードから明示的に渡します（環境変数と環境設定ファイルから作る場合は、後述の `LoadEnvConfig` を明示的に呼びます）。
- 作成後は、そのロガーが使う設定を変更しないでください。
- `New`・`NewAsync`・`NewDelivery` に渡したcontextのキャンセルと期限は、構築処理だけに適用します。構築後にcontextが終了しても、バッファの定期送信は `Close` / `Shutdown` まで継続します。

### 環境変数からの設定

`LoadEnvConfig` は、`HETEROLOG_` で始まる環境変数と、引数で指定した環境設定ファイルから設定を読み込みます。設定のキーは、既定値として参照するAWSの標準の変数（`AWS_ACCESS_KEY_ID` など）を除き、すべて `HETEROLOG_` で始まるため、アプリケーション自身の設定と同じ環境変数やファイルに置けます。すべてのキーは [.env.example](../.env.example) にあります。値は設定の一例で、既定値と異なる項目があります。出力先ごとの既定のログレベルとサンプリングは、READMEの[既定のログレベルとサンプリング](../README.ja.md#既定のログレベルとサンプリング)にあります。何も設定しなければ、3種類のロガーとも標準出力だけに記録します。復旧CLIも同じキーを読みます。

例は、READMEの[複数の出力先へ送る](../README.ja.md#複数の出力先へ送る)にあります。

- プロセスの環境変数は変更しません。同じキーが環境変数とファイルの両方にある場合は、環境変数の値を使います。
- 指定したファイル以外は読みません（`HETEROLOG_ENV_PATH` と現在のディレクトリの `.env` も読みません）。
- 有効にしたロガーの設定と、ロガー間の整合性（保存先の重複など）を読み込み時に検証し、見つかった問題をまとめて返します。`errors.As` で `EnvFileError`・`InvalidEnvironmentValueError`・`RenamedEnvironmentKeyError`・`pkg/env` の `VariableError` を取り出すと、問題のキーを特定できます。
- 設定を変えてからロガーを作る場合は、`cfg, opts, err := env.Config()`（`AsyncConfig`・`DeliveryConfig` も同様）で取り出し、`heterolog.New(ctx, cfg, opts...)` に渡します。`opts` には、環境設定から読んだspoolの再送設定とCloudWatch Logsのstream分割が入ります。
- `HETEROLOG_ENABLE_LOGGING=false` などで無効にしたロガーは作れず、`ErrEnvLoggerDisabled` を返します。
- トレース（`WithObservers`）は環境設定から作りません。`env.New(ctx, opts...)` のように追加のoptionとして渡します。`HETEROLOG_USE_MEMORY_PRESSURE_*` で有効にしたspoolは、`WithMemoryPressureMonitor` を渡さなければ、`HETEROLOG_MEMORY_CRITICAL_RATIO`・`HETEROLOG_MIN_AVAILABLE_MEMORY_BYTES_MB`・`HETEROLOG_MEMORY_CRITICAL_CONSECUTIVE_HITS` でメモリ逼迫を判定します。

### 稼働中の設定変更

同期・非同期のロガーの出力先ごとのログレベルとサンプリングは、アプリケーションの稼働中に、コードまたはHTTP APIから変更できます。

- コードでは、`runtime.OutputSettings()` が現在の設定を返し、`runtime.UpdateOutputSettings(changes)` が設定を変えて、変えた出力先ごとに、反映の直前と直後の設定（`AppliedOutputSettings`）を返します。変更は、指定したレベルとサンプリングの項目だけを変えます（`OutputSettingsChange` と `SamplingChange`）。ほかの項目は反映する時点の値のままなので、コードやHTTP APIからの同時の変更が、互いを打ち消しません。すべての変更を先に検証し、一つでも不正なら何も反映しません。ロガーが使っていない出力先は `ErrUnknownOutput`、不正な値は `pkg/logger` の `ConfigFieldError` です。
- 変更は次のログから反映します。非同期のロガーは、ログを処理する時点の設定を使います。
- 変更はメモリ上だけに保持するため、再起動すると設定どおりに戻ります。
- Deliveryのロガーには、この設定はありません。サンプリングはせず、レベルで絞り込むのはSlackの出力だけで、そのレベルは設定どおりです。内部ログの通知だけに使う出力先は、設定どおりのままです。

HTTP API（管理用API）は、curlなどのツールから呼べます。

- `NewAdminHandler` は、アプリケーション自身のサーバーに組み込む `http.Handler` を返します。代わりに `StartAdminServer` で専用のサーバーを起動することもできます。どちらにも、[監査ログ](#管理用apiの監査ログ)を記録する送達確認ロガーを渡します。
- 管理用APIに渡すロガーの出力先は、それぞれ自分の設定を持つ必要があります。二つの出力先が `pkg/logger` の同じ `LiveSettings`（`RuntimeConfig.Live` で設定）を共有している場合は、同じロガーの中でも別のロガーの間でも、また一つのロガーを二つの名前で渡した場合も、`NewAdminHandler` と `StartAdminServer` は `ErrAdminSharedLiveSettings` を返します。一方への変更がもう一方も変え、その変化を監査ログに記録することも、要求と照合することもできないためです。確かめるのはheterologが作るロガーで、`SettingsController` を独自に実装したものは確かめません。環境設定から作るロガーが設定を共有することはありません。
- `GET /settings` はすべてのロガーの現在の設定を返し、`PATCH /settings` はJSONの本文で指定した項目を変えます。レベルは `DEBUG`・`INFO`・`WARN`・`ERROR`（大文字小文字を区別しません）で、`PATCH` で省略したサンプリングの項目は、別の `PATCH` が同時にほかの項目を変えても、今の値のままです。
- すべての要求で `Authorization: Bearer <トークン>` が必要です。トークンは32バイト以上（`MinAdminTokenLength`）で、例えば `openssl rand -hex 32` のように無作為に作ります。比較は一定時間で行い、エラーにトークンを含めません。
- `PATCH` は1件ずつ順に処理し、処理中に届いた要求は、前の要求が終わるまで待ちます。`PATCH` は、すべてのロガーのすべての変更を検証してから反映します。本文は64KiBまでで、未知の項目は拒否します。
- どの応答にも、監査ログでの要求IDを `X-Request-Id` ヘッダーで返します。
- 専用のサーバーは、アドレスのホストが空の場合（例: `:9100`）は127.0.0.1で待ち受けます。ほかのアドレスで待ち受ける場合は、ホストを明示し、TLSの証明書と鍵を設定します。トークンをネットワーク上に平文で流さないためです。
- 環境設定では、`HETEROLOG_ADMIN_ADDR` でサーバーを有効にし、`HETEROLOG_ADMIN_TOKEN`・`HETEROLOG_ADMIN_TLS_CERT_FILE`・`HETEROLOG_ADMIN_TLS_KEY_FILE` とともに設定します。`LoadEnvConfig` が検証し、`env.AdminServerConfig()` が返します。

curlを使った例は、READMEの[稼働中にログレベルとサンプリングを変える](../README.ja.md#稼働中にログレベルとサンプリングを変える)にあります。

### 管理用APIの監査ログ

管理用APIは、変更の要求をすべて監査ログに記録します。監査ログは専用の送達確認ロガーが書くため、あとから変更を確かめられ、管理用APIが変えるレベルやサンプリングで記録が隠れることもありません。

- `PATCH` は、次の段階ごとに記録し、要求IDで結びます。

  | 段階 | レベル | 記録する時点 |
  | --- | --- | --- |
  | `requested` | INFO | 要求が届いた時点。要求した項目（`changes`）を含めます |
  | `started` | INFO | 変更を始める時点。変更する出力先の設定（`settings`）を含めます |
  | `applied` | INFO | 変更を反映した時点。変更後の設定（`settings`）と、要求どおりか（`verified`）を含めます |
  | `mismatch` | ERROR | 変更後の設定が、始める時点の設定に要求を適用した設定と違う出力先ごと（`expected`・`actual`） |
  | `rejected` | WARN | 要求を拒否した時点。理由（`reason`）を含めます。トークンがない・違う要求や、ほかの経路・方法への要求も含みます |
  | `failed` | ERROR | 検証を通った変更を反映できなかった時点 |
  | `undelivered` | ERROR | その要求の記録の一部が、監査用ロガーの出力先のすべてに届かなかった時点（`undelivered_phases`） |

- どの記録にも `log_kind=audit`・`audit_phase`・`remote_addr` のフィールドと、ログの要求IDとしての要求IDが付きます。形式は出力先ごとの作法に従います（例: ファイルでは1行のテキスト、OpenSearchではJSONの文書）。記録しないのは、成功した `GET` だけです。
- 食い違いがあっても変更は戻さず、応答は `500` で今の設定を返します。記録が届かなくても変更は止めず、応答は変更の結果のまま警告を含めます。届かなかった記録は、送達確認ロガーの通常の動きとして、失敗時の保存先（failure archive）に残ります。
- 環境設定では、`HETEROLOG_ADMIN_ADDR` を設定すると、`env.NewAdminAudit(ctx)` が監査用ロガーを作り、`env.AdminAuditConfig()` がその設定を返します。コードでは、heterologが作った送達確認ロガー（`*DeliveryRuntime`）を `NewAdminHandler` か `StartAdminServer` に渡し、管理用APIを止めた後で閉じます。

環境設定から作る監査用ロガーは、接続先・認証・タイムアウトなどに送達確認ロガーの設定（`HETEROLOG_DELIVERY_*`）を使います。`HETEROLOG_ENABLE_DELIVERY_LOGGING` がfalseでも同じです。変えるのは次の点だけです。

- 出力先: `HETEROLOG_ADMIN_AUDIT_LOG_TYPE`。監査ログは欠けてはいけないため、ほかのロガーと違い、既定は `stdout` と `file` の両方です。`slack` を加えると、送達確認ロガーのSlackの出力は既定でERRORだけを記録するため、ERRORの記録が通知になります。
- ファイル: `HETEROLOG_ADMIN_AUDIT_FILE_LOG_DIR`（既定は `/audit_logs`）。ほかのログのディレクトリと同じく、`HETEROLOG_SYSTEM_LOG_DIR` と `HETEROLOG_TARGET` を基準に解決します。一つの要求の記録が分かれないよう、すべてのレベルを同じファイルに書き、保守での削除・圧縮・アップロードはしません。ほかの保存先と重なるディレクトリは使えず、`LoadEnvConfig` が `StorageDirectoryCollisionError` を返します。
- 出力先ごとの記録先: `HETEROLOG_ADMIN_AUDIT_SLACK_CHANNEL`（すべてのレベル）・`HETEROLOG_ADMIN_AUDIT_OPENSEARCH_INDEX`・`HETEROLOG_ADMIN_AUDIT_LOKI_LABELS`（ラベルのJSONオブジェクト）・`HETEROLOG_ADMIN_AUDIT_CLOUDWATCH_LOG_GROUP_NAME`・`HETEROLOG_ADMIN_AUDIT_CLOUDWATCH_LOG_STREAM_NAME`・`HETEROLOG_ADMIN_AUDIT_CLICKHOUSE_DATABASE`・`HETEROLOG_ADMIN_AUDIT_CLICKHOUSE_TABLE`。空の項目は送達確認ロガーの設定のままです。監査ログをほかのログと分けるには、これらを指定します。
- 失敗時の保存先（failure archive）は、専用のディレクトリ `admin_audit_logger_<backend>_failure_archives` を使い、アップロードしません。送達確認ロガーの内部ログ専用の出力先（`HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_*`）は使いません。

### メモリ逼迫の監視器

- `WithMemoryPressureMonitor` に渡す監視器は、呼出側で起動・停止します。
- `WithMemoryPressureMonitor` を渡さない場合は、`UseMemoryPressureForOverload` を有効にした各spoolが自分の監視器を起動します。OSのメモリを1秒ごとに取得し、spoolの `MemoryCriticalRatio`・`MinAvailableMemoryBytes`・`MemoryCriticalConsecutiveHits`（環境設定では `HETEROLOG_MEMORY_CRITICAL_RATIO`・`HETEROLOG_MIN_AVAILABLE_MEMORY_BYTES_MB`・`HETEROLOG_MEMORY_CRITICAL_CONSECUTIVE_HITS`）で判定し、ロガーの終了で停止します。閾値を一つも有効にしない場合や、メモリを取得できない場合は、ロガーの起動がエラーになります。
- `NewMemoryPressureMonitor(provider, policy, pollInterval)` は、`pkg/memorypressure` の `Provider` と `CriticalPolicy` から監視器を作ります。
- `Start(ctx)` は一度取得してから戻り、以降は `pollInterval` ごとに取得し、`ctx` の終了で停止します。
- 判定は `memorypressure.CriticalState` と同じです。信頼できる観測が `CriticalRatio` または `MinAvailableBytes` に達した状態が `ConsecutiveCriticalHits` 回（0と1は初回）続くとcriticalです。
- providerがnil、取得間隔が0以下、どちらの閾値も無効、1を超える `CriticalRatio`、負の連続回数は、生成時にエラーになります。生成時の入力エラーは `errors.Is(err, heterolog.ErrInvalidMemoryPressureMonitor)` で判別できます。
- 渡した監視器もspoolが起動した監視器も、spoolの `UseMemoryPressureForOverload` を有効にした出力先の過負荷判定に使われます。過負荷と判定した場合はoverload policyに従い、`drop` では保存せずに破棄して、通常APIへ `ErrLogDropped` を返します。
- `pkg/memorypressure` のLinux providerは、所属するcgroup v2と参照可能な親階層の上限を確認します。使用率が最大の階層を主観測、残量が最小の階層を `CgroupLimit` として保持し、判定では両方を確認します。有限の上限だけが読めた場合は、使用量不明として保持します。名前空間の外など参照できない祖先の制約は取得できません。

### 内部ログの通知

- 内部ログの通知先（`HandlerConfig` の `OnErrorConfig`・`OnWarnConfig`・`OnInfoConfig`・`OnDebugConfig`）には、通知専用の書き込み経路を使います。
  - `file` は、fileロガーへ内部ログを通知しない経路で書き込みます。
  - `stdout`・`slack`・`opensearch`・`loki`・`cloudwatchlogs`・`clickhouse` は、通常の出力とは別に作る通知専用のロガーを使います。
  - これらの書き込みで起きた失敗は通知せず、失敗ごとに標準のloggerへ一行出力します。
- 通知先に選んだ出力先の設定は、通知の初回ではなく `New`・`NewAsync`・`NewDelivery` の構築時に、保存先を作る前に検証します。
  - 通知専用の書き込み先を作れない設定は構築時の設定エラー（`ErrInvalidNotificationTarget`）になり、通知の時点で通知を捨てることはありません。
  - 通知先に選んでいない出力先の設定は、この検証の対象にしません。
  - `file` の通知先は、時刻表示の設定（timezone・日時の書式）を構築時に検証し、誤りは構築時の設定エラーにします。
- `file` の通知先のログファイルは、最初の通知で作成します。作成に失敗した通知は失敗ごとに標準のloggerへ一行出力し、次の通知で作成を再試行します。
- 出力先に自分自身を通知先として選んでも（例: fileロガーの `OnErrorConfig` に `file`）、通知が繰り返されることはありません。
- 仮に通知の処理の中から同じロガーへの通常の書き込みが起きた場合も、その書き込みで生じた内部ログ（遅延・失敗・過負荷など）は通知先を再び呼ばず、そのロガーの出力へ直接書き込みます（fileは同じログファイル、stdoutは標準出力・標準エラー、Slack・OpenSearch・Loki・CloudWatch Logs・ClickHouseは同じ送信先）。この直接の書き込みにもそのロガーのlog levelを適用し（fileは過負荷時の処置も適用します）、その失敗は通知しません。
- 送信をまとめるロガーとSlackロガーは、内部ログの通知を排他の解放後に行います。非同期API（`NewAsync`）は、実行枠がすべて使用中でも通知の中からの書き込みを待たせずに処理します。そのため、通知の中から同じロガーへ書き込んでも停止しません。

## ログファイル

### ローテーション

- ローテーション先を開けなかった場合、その書き込みはエラーを返します。既存ログを保持し、障害解消後の次の書き込みでローテーションを再試行します。
- ローテーション前のファイルを閉じる処理がエラーを返した場合も、その書き込みはエラーを返します。そのファイルは再利用せず、次の書き込みで新しいファイルを開いてローテーションを完了します。
- ローテーション処理は標準出力へ診断文字列を直接書き込みません。ファイルだけを出力先に選んだ場合も同じで、通常API・非同期API・Delivery APIで共通です。

### 保存先パスと所有権

- 保存先パスに含まれる有効な空白は、そのまま扱います。
- 保存先の所有権は実際のパスで判定します。同じディレクトリや親子ディレクトリを別のロガーが同時に所有する構成は拒否します。

### 大文字小文字の区別

- ログファイル名の照合は、保存先ディレクトリのファイルシステムが大小文字を区別するかに従います。
- Windowsと、macOSの大小文字を区別しないボリューム、Linuxで大小文字を区別しないと確認できたディレクトリでは、prefix・target・拡張子の大小文字だけが異なる名前を同じファイルとして扱います。これをローテーション時の世代番号の選択、圧縮・アップロード・削除対象の検出、アップロード記録（checkpoint）の系列の識別に使います。
- failure archiveなどの保存先パスにtarget名を付けるとき、既存の階層に同じ名前があるかは、各階層のディレクトリの大小文字の規則で判定します。大小文字を区別しないボリュームでは、大小文字だけが異なる名前も同じディレクトリとみなし、targetを二重に付けません。
- Linuxでは、次のディレクトリを大小文字を区別しないと判定します。
  - casefold属性を持つディレクトリ（ext4・F2FS・tmpfsのcasefold）。
  - FAT（vfat・msdos）・exFATのファイルシステム上のディレクトリ。ただし、大小文字を区別するマウントオプション（vfatの `check=s`〔strict〕、msdosの `nocase`、Linux 5.4〜5.6のstaging exFATドライバーの `namecase=1`）を指定していないことを `/proc/self/mountinfo` で確認できた場合だけです。確認できない場合は区別するものとして扱います。
- 判定はファイルシステムの種類・属性・マウント情報を読み取るだけで、確認用のファイルは作成しません。
- ext4・F2FS・tmpfs上の読み取り権限のないディレクトリは、casefold属性を確認できないため、大小文字を区別するものとして扱います。
- マウントやcasefold属性の変更は、反映までに最大2秒かかることがあります。
- SMB/CIFS・NFS・NTFS（ntfs3・ntfs-3g）・FUSE（exfat-fuseを含む）・9p（WSL2のWindowsドライブなど）・ZFSなどは、大小文字の扱いがサーバーやマウントオプションによって変わり外から確認できないため、大小文字を区別するものとして扱います。これらのマウントが大小文字を区別しない場合は、大小文字だけが異なる設定を使わないでください。
- 通常ログと内部ログの圧縮ログ名の衝突検証では、元ログの保存先またはアップロード失敗時の退避先のいずれかが大小文字を区別しない場合、大小文字だけが異なる名前も衝突として拒否します。

### macOSのAppleDoubleファイル

- macOSは、拡張属性を保存できないファイルシステム（FAT32・ExFATなど）では、ファイルやディレクトリの拡張属性を、同じディレクトリの `._` + 名前のファイル（AppleDouble）に保存します。
- macOSでは、同じディレクトリに対応する名前のファイルやディレクトリがある `._` のファイルを、ログ・spool・failure archiveのファイルとして扱わず、容量の計算や送信・削除の対象から除きます。対応する名前がない `._` のファイルは、通常のファイルとして扱います。
- macOS 26のExFATは既存のファイルを置換しないrenameに対応していないため、spoolとfailure archiveの保存先には使えません。その場合の保存は、既存のファイルを上書きせずにエラーになります。

### 保持

- ログファイルの保持世代数（`HETEROLOG_MAX_LOG_FILE_BACKUPS` など）は、ファイル名の日付と番号で決まる世代単位で数えます。
- 同じ世代の未圧縮ファイルと圧縮ファイルは、圧縮形式の変更前に作られたものも含めて一世代として数え、上限を超えた古い世代のファイルはまとめて削除します。
- 書き込み中のファイルや送信・退避が未完了のファイルなど、削除できないファイルは世代数に含めません。
- Deliveryのrunnerモードで `LifecycleRunnerSettings` に指定する `MaxFileBackups`・`MaxFileAge`・`MaxDirSize` は、通常のfile loggerと同じ下限（世代数2以上・日数1以上・容量10MB以上かつファイル最大サイズ以上）で検証します。error・debugの分離を無効にした場合も、不正な値ではロガーの構築に失敗します。

### 圧縮

- 圧縮済みファイルを復旧に使う前に、内容の完全性を検証します。XZでは各ストリームのindex・footerとブロックの終端位置まで確認するため、途中切れの圧縮ファイルが残っていても元ログを保持して再圧縮できます。
- 元ログと同じ世代の圧縮済みファイルが残っている場合、組み込みの圧縮形式（gzip・zstd・lz4・xz）では最後まで展開しながら元ログと内容が一致することを確認し、一致した場合だけ元ログを削除します。
- 圧縮済みファイルが完全でも内容が異なる場合は、元ログを削除せずログディレクトリの `.preserved` へ退避してエラーとして通知し、圧縮済みファイルはその世代のファイルとして扱います。
- 独自codec（`CompressionCodec`）の圧縮済みファイルは、展開に使えるmethodを持っていても内容を比較せず、codec自身の完全性検証だけを行います。
- 組み込みの圧縮形式で新しく圧縮する場合は、公開前の一時ファイルを最後まで展開して元ログと内容が一致することを確認します。一致しなければ圧縮ファイルを公開せず、元ログも削除しません（元ログは次の処理で再圧縮します）。独自codecでは従来どおりcodec自身の完全性検証だけを行います。
- ローテーション直後の圧縮や中断した処理の再開で破損した圧縮済みファイルを見つけた場合も、元ログを残して破損したファイルを削除し、元ログから再圧縮します。そのため、終了処理が同じ失敗の再試行を待ち続けることはありません。
- POSIX環境の圧縮出力は、圧縮元のファイル権限より広くならないように作成します。既存の一時出力やumaskによる追加の制限も保持します。
- Windowsでは圧縮データを書き込む前に、現在のユーザー・SYSTEM・Administratorsだけにアクセスを許可する既存のprivate-file方針を適用します。

### object storageへのアップロード

- ログファイルのobject storage uploadに宛先を追加した場合、追加前から存在したログファイルはその宛先へ送信せず、送信済みとしても扱いません。
- ただし、追加時点で現在のローテーション期間に書き込み中の最新ファイル（未圧縮）は、その後も追記されるため、追加した宛先の送信対象に含めます。過去の期間の未圧縮ファイルは、最新のファイルでも追加前のファイルと同じく対象外です。
- 宛先の追加を理由に削除することはなく、通常の保持期間・世代数・容量の設定に従って削除されます。
- upload後に圧縮ファイルを削除するのは、有効な宛先の少なくとも一つへ送信（失敗時は指定した全退避先へ保存）し、残りの有効な宛先でも送信済みか、追加前の対象外と確認できた場合だけです。
- failure archive（fileArchive）のobject storage送信では、古い世代の送信が完了するまで新しい世代を待機させ、古い世代の完了記録が進んだ時点で直ちに送信します。

### 再起動

- ログファイルのwriter（通常・Delivery・backend送信失敗のfileArchive）は起動時、保存先に残るファイルに加えて、同じ系列のcheckpointに記録された世代（送信・退避の完了位置と送信途中の記録）も使用済みとして扱い、その後の番号から書き込みます。
- そのため、手動復旧で最新のファイルまで送信・削除した後に再起動した場合や、ログ保存先だけが失われた場合も、新しいログが送信済みの世代と同じ名前になることはありません。同じ名前になると、送信されないまま削除されたり、object storageの同じobjectを上書きしたりするためです。
- システム時計が記録済みの期間より前へ戻った場合は、記録済みの期間を続けて書き込みます。

## 失敗時の扱いと再送

### spoolの権限

- 失敗ログのspool（OpenSearch・Loki・ClickHouse・CloudWatch Logs）は、spoolのルートと配下のディレクトリを所有者だけがアクセスできる0700で作成し、spoolファイルを0600で保存します。umaskによる追加の制限は保持します。
- 以前のバージョンが広い権限で作成した既存のディレクトリも、ルートから保存先までのgroup・otherの権限を外します。ルートより上位にある利用者のディレクトリは変更しません。
- ルートより上位で不足するディレクトリは、object storageのspoolなど他の保存先と共有するため、以前と同じ0755（umaskを適用）で作成します。
- Windowsでは、新しく作成するディレクトリに現在のユーザー・SYSTEM・Administratorsだけを許可するACLを設定し、spoolファイルは作成時からこのACLを継承します。既存ディレクトリのACLは変更しないため、以前のバージョンが作成したspoolディレクトリのACLは必要に応じて見直してください。

### 失敗の分類

失敗ログのspool（OpenSearch・Loki・ClickHouse・CloudWatch Logs）への保存と再送は、通常送信とspool再送で同じ規則に従います。

- 一時的な障害として自動再送するもの: 408・429・5xx、接続失敗やタイムアウトなどの既知の通信障害、circuit breakerによる送信抑止、Lokiの260。
- 応答の欠落・解析失敗、送信中の中断、分類不能の送信エラーは、結果を確認できないため `quarantine` へ隔離し、内容と配送状況を確認するまで自動再送しません。
- HTTP 413などのサイズ超過と、ログの内容による拒否（OpenSearchのmapping・解析エラーや、ClickHouseの値の解析・型変換エラーなど）は、複数件の送信なら分割または一件ずつ再送して原因のログを特定し、一件でも拒否されたログを `quarantine` に保存します。
- Lokiの400は有効なstreamを一部受理した可能性があるため、分割せずに、送信したログを `quarantine` に保存します。
- 認証・権限の拒否、存在しないtable・index・stream、SQLや設定の誤りなど、ログの内容に依存しない宛先・設定の不備は、配送不能として `dead` に保存します。
- CloudWatch Logsが受付期間外として個別に拒否したイベントは、未来すぎるものを再送対象とし、古すぎるもの・保持期間外のものを `dead` に保存します。
- 再送時は、以前の試行が受理されていた場合に重複配送が起こり得ます。
- circuit breakerが送信を再開できるか確かめる試行では、送信先の応答を確認できた場合だけ送信抑止を解除します。clientが応答もエラーも返さなかった場合は、結果不明として送信抑止を続けます。
- 再送時にspoolのrecordを復元・検証できない場合は `quarantine` へ隔離します。このときのerrorは種類（sentinel）とfield名で判定できますが、保存された値は `errors.As` で取り出せるerror（原因やfield）に含めません。

### Retry-After

- OpenSearch・Loki・ClickHouse・CloudWatch Logsの送信先が再送可能な失敗とともに `Retry-After` を返した場合は、応答を受け取った時刻から指定時間が経つまで、その出力先へspoolから再送しません。
- 指定は、spool再送で受け取った場合も通常送信で受け取った場合も出力先全体の待機期限として記録し、期限を持たない古いログも含め、その出力先のログを期限まで送りません。
- 通常送信で受け取った指定は、失敗したログをspoolへ保存する時点（過負荷で `drop`・`degrade` してspoolへ保存しない場合も含みます）で記録するため、稼働中のrunnerも次の周期から適用します。
- spool再送で受け取った場合は、同じ再送で未送信だったログも送りません。
- 新しい指定は、記録済みのより後の期限を短縮しません。
- 他の出力先の再送と、spoolの保持期間・容量による削除や後処理の再試行は続けます。
- 待機時間の上限は既定で1時間で、backend別の `Worker.MaxRetryAfter`（環境変数 `HETEROLOG_OPENSEARCH_SPOOL_RUNNER_MAX_RETRY_AFTER_SECOND` などの `*_SPOOL_RUNNER_MAX_RETRY_AFTER_SECOND`）で変更できます。より長い指定は上限までに短縮し、内部ログへ警告します。
- 指定が再送の周期より短い場合は、従来どおりの周期で再送します。
- 一回の送信の中で行うclient retryは、通常送信・spool再送ともに `MaxDelay` まで待機します。

待機期限は再起動後も引き継ぎます。

- 待機期限はspoolディレクトリ（`<SpoolPath>/<Target>/spool/<Dir>`）内の `.retry-after/<backend名>.json`（例: `opensearch.json`）に保存し、再起動後は最初の再送より前に読み込みます。そのため再起動後も、ログを読み込む順序にかかわらず期限まで再送しません。
- 指定時刻は再送待ちのログにも保存します。期限の記録を保存できなかった場合は、そのログを読み込んだ時点で期限を復元します。
- 読み込んだ時点から上限より先の時刻を保存したログ・待機期限は、上限までに短縮して内部ログへ警告します。短縮した期限も保存するため、再起動しても待機は延びません。
- 期限を過ぎた記録は削除します（上限までに短縮したログの元の時刻が過ぎるまでは、短縮済みであることを示すために残します）。
- 保存の途中で停止した場合に残る一時ファイル（`.<backend名>.json.<数字>.tmp`）は、次回の読み込み時に削除します。
- 記録を読み込めない場合や内容が壊れている場合は、内部ログへ警告したうえで記録がないものとして再送を続け、次の `Retry-After` で置き換えます。
- 記録の保存に失敗した場合は内部ログへ警告し（失敗が続く間は一度だけ）、プロセス内では期限まで待機しながら、期限が来るまで保存を再試行します。再試行の間隔は再送の周期から始めて失敗のたびに倍にし、最大1分です。保存できないまま再起動した場合は、期限を引き継げない場合があります。

### 圧縮ログのアップロード

- 圧縮ログファイルをobject storageへ再送するspoolは、保存時と同じ宛先・保存名へファイルを送り直します。そのため、分類不能の送信エラーやアップロード処理のpanicでも `quarantine` へ移さず、再送待ちに戻して自動再送します。`dead` へ移すのは、送信先の応答から恒久的な拒否と判定できた場合だけです。
- 圧縮ログファイルの送信先が `Retry-After` を返した場合は、その宛先のファイルだけを指定時刻まで送信せず、他の宛先の再送は続けます。
- spoolへ保存する前の通常のuploadが一時的な失敗とともに受け取った指定も、ファイルをspoolへ保存する時点でその宛先の待機期限として記録します。そのため、稼働中の再送処理も次の周期から、期限までその宛先のファイルを送信しません。
- 新しい指定は、記録済みのより後の期限を短縮しません。
- 待機時間の上限は既定で1時間で、`HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_MAX_RETRY_AFTER_SECOND`（Delivery loggerは `DELIVERY_` で始まる同名の設定）で変更できます。より長い指定は上限までに短縮して警告を通知します。
- 待機期限は宛先別のspoolディレクトリ内の `.retry-after/<宛先ID>.json`（`<SpoolPath>/<Target>/spool/objectstorage/<service>/<logger契約>/<宛先ID>/.retry-after/<宛先ID>.json`）に保存し、再起動後はその宛先へ最初に送信する前に読み込みます。そのため、期限までその宛先のファイルを送信しません。
- 宛先IDは、宛先別のspool・file archiveのディレクトリと `.retry-after/<宛先ID>.json` の名前に使います。そのため、英小文字・数字で始まり、英小文字・数字・ハイフン・アンダースコアだけからなる128文字以下で、Windowsの予約デバイス名（`con`・`prn`・`aux`・`nul`・`com0`〜`com9`・`lpt0`〜`lpt9`）でない値に限ります。S3の宛先ID（`s3-` に続くSHA-256の16進数64文字）は常にこの規則を満たします。
- upload失敗時の保存先（spoolまたはfile archive）を使う構成で規則に合わない宛先IDがあると、どの保存先も作成せずに、ロガーの構築時に設定エラーを返します。
- 読み込んだ時点から上限より先の時刻は、上限までに短縮して保存し直し、警告を通知します。そのため、再起動しても待機は延びません。
- 期限を過ぎた記録と、保存の途中で停止した場合に残る一時ファイル（`.<宛先ID>.json.<数字>.tmp`）は削除します。
- 記録を読み込めない場合や内容が壊れている場合は、警告を通知したうえで記録がないものとして再送を続け、次の `Retry-After` で置き換えます。
- 記録の保存に失敗した場合は警告を通知し（失敗が続く間は一度だけ）、プロセス内では期限まで待機しながら、期限が来るまで保存を再試行します。再試行の間隔は再送の周期から始めて失敗のたびに倍にし、最大1分です。保存できないまま再起動した場合は、期限を引き継げない場合があります。

### 永続性とキャンセル

- OpenSearchとClickHouseのspool復元はJSON数値の精度を保持します。大きな整数や高精度の小数を再送のために `float64` へ変換せず、保存済みのdocument IDや重複排除tokenも維持します。
- fileArchiveへの追記が途中で失敗した場合は、その追記を始める前の位置まで戻します。復旧できない間は後続の保存・ローテーションを止めてエラーを返し、次回保存時に復旧を再試行します。再起動時に既存ファイルの末尾が不完全だった場合は、内容を保持したままオープンを拒否するため、ファイルの確認・修復が必要です。
- OpenSearch・Loki・ClickHouse・CloudWatch Logsの通常送信で期限切れやキャンセルが起きても、失敗ログの保存は独立した期限で試みます。
- 通常送信・spool再送ともに、送信中に呼び出し元の期限切れ・キャンセルで結果が不明になったログは `quarantine`、まだ送信していないログは再送対象として扱い、成功が確定したログは再投入しません。明示的な応答が得られた場合は、その結果を維持します。
- CloudWatch Logsでは、途中の退避が失敗しても送信contextが有効なら後続の送信・退避を続け、各処理のエラーをまとめて返します。
- HTTP応答のステータスと `Retry-After` は、応答本文に含まれる通信障害らしい語句によって上書きしません。
- `Retry-After` が待機時間として表せないほど大きい場合も即時再送にはせず、最大値として扱ったうえで `MaxDelay` などの待機上限を適用します。
- 接続のリセット・中断・拒否は、OSごとに異なるエラーメッセージではなくエラー番号で判定するため、Windowsでも一時的な切断として再試行します。
- バックエンド固有のエラー種別は、各サービスの再送規則に従って判定します。
- HTTP拒否応答を受け取った後、次の送信までの待機中にキャンセルされた場合は、確定した応答情報とキャンセル原因を両方保持します。503などの一時的な拒否は再送対象として保存し、送信中の中断など結果が不明なログはquarantineへ隔離します。
- OpenSearchのingest pipelineが正常に破棄したログの `noop` 応答は、処理完了として扱います。同じバッチの他のログは、それぞれの応答に従って配送結果を判定します。
- `done`・`dead`・`quarantine` への保存後に元の `processing` ファイルの削除が失敗しても、再起動時に保存内容を照合し、単一の状態に全件が保存済みと確認できれば元ファイルだけを削除します。一部だけの保存、複数状態への分割、内容の不一致はファイルを保持してエラーを返し、自動再送を止めます。この確認に必要な保存ファイルは、対応する元ファイルが残る間、保存期間・容量による削除から保護されます。
- 再送結果の保存も、ファイルと保存先ディレクトリを同期してから完了とします。公開後の同期失敗では保存済みファイルと元の `processing` を保持し、再試行時に内容を照合して同期をやり直します。既存の結果を上書きせず、最初の確定時刻を保持します。
- spoolの後処理に失敗した場合、状態遷移の `RetryDelay` は失敗が判明した時点から数えます。ファイル操作に時間がかかった場合も、次の再試行まで設定した待機時間を確保します。
- 通常ログのspoolは、ファイルと保存先ディレクトリの同期が確認できてから保存成功を返します。公開後の同期に失敗した場合も、保存したファイルは保持します。このエラーを受けて同じログを再度保存すると、欠落を避けるため別ファイルに保存され、重複が生じる場合があります。
- spool・fileArchive・checkpointの保存先ディレクトリを新しく作成した場合も、親ディレクトリの同期を確認してから成功を返します。親の同期に失敗した場合は `ErrDurabilityUnconfirmed` を返し、同じプロセスでその親に依存する保存先を準備するたびに同期を再試行します。成功するまでは、作成済みのディレクトリでも同じエラーを返します。
- spoolの容量統計は `StatsScanInterval` が0以下でも更新します。初回は同期して走査し、以降は非同期の再走査結果を容量判定に反映します。`MaxCallsSinceScan=0` は呼び出し回数による更新だけを無効にします。

### タイムアウトの予算

- `must_fit` のタイムアウト予算検証では、LokiとClickHouseの通常retryでも `Retry-After` による待機を `MaxDelay` まで含めます。
- 環境設定でloggerやclientのtimeout・retry設定を省略した場合の既定値は `.env.example` の値と同じで、既定の `must_fit` を満たします（通常・非同期loggerは18秒、Deliveryは12秒、OpenSearchのclientは1秒、Loki・ClickHouse・CloudWatch Logsのclientは2秒）。
- 環境設定から読み込む復旧CLIは、復旧を始める前にこの予算を含む設定全体を検証し、満たさない設定は設定エラーで終了します。

## メトリクスとトレース

`WithObservers` で `Tracer` と `Meter` を渡すと、以下のメトリクスとspanを受け取れます。渡さなければ何も記録しません。

### メトリクス

- 名前はPrometheusの慣習に従います。`heterolog_` で始まり、counterは `_total` で終わり、単位を名前に含めます。時間の単位は秒です。
- `backend` 属性は、`pkg/logger.LogOutputDestination` の名前（`opensearch`・`slack` など）です。
- spoolからの再送も、通常の送信と同じ種類のretry clientとcircuit breakerを通るため、そのリクエストはretryとcircuit breakerのメトリクスに含まれます。再送したrecordは `heterolog_spool_replay_records_total` が数えます。ほかの出力先のメトリクスは通常の送信だけを数えます。
- 内部ログの通知は、`heterolog_backend_records_total` とflushのメトリクスに数えません。`heterolog_internal_logs_total` が数えます。

| メトリクス | 種類 | 属性 | 意味 |
|---|---|---|---|
| `heterolog_backend_records_total` | Counter | `backend`・`outcome` | OpenSearch・Loki・ClickHouse・CloudWatch Logs・Slackへ送ったrecordの数を、結果ごとに数えます。結果は `success` か、[失敗の分類](#失敗の分類) の `retryable`・`dead`・`quarantine` です。Slackは、送れなかったrecordを次のbatchへ残し、送るたびに `retryable` として数えます。bufferが一杯で捨てたrecordは `dropped` です。Slackへの送達（delivery）の失敗は `dead` です。fileロガーは、1件の書き込みごとに `success` か `failure` として数えます。過負荷時の方針 `drop` で破棄したentryは、代わりに `heterolog_file_dropped_entries_total` に数えます。 |
| `heterolog_backend_request_duration_seconds` | Histogram | `backend`・`outcome`（`success`・`failure`） | 1回のリクエストにかかった時間です。retry clientの再試行と、その間の待ち時間を含みます。一部のrecordだけを拒否した応答は `success` で、そのrecordは `heterolog_backend_records_total` に数えます。 |
| `heterolog_backend_flush_records` | Histogram | `backend` | bufferからまとめて送る1回分のrecordの数です。 |
| `heterolog_backend_flush_bytes` | Histogram | `backend` | bufferからまとめて送る1回分のbyte数です。`BufferFlushBytes` と比べるために数えた値です。Slackは記録しません。 |
| `heterolog_backend_retries_total` | Counter | `backend`・`reason` | retry clientが再試行した回数を、再試行の理由（`http_throttling`・`net_timeout` など）ごとに数えます。 |
| `heterolog_circuit_breaker_rejected_requests_total` | Counter | `backend` | circuit breakerが送らずに断ったリクエストの数です。 |
| `heterolog_circuit_breaker_transitions_total` | Counter | `backend`・`state`（`open`・`closed`） | circuit breakerが開いた回数と、復旧確認（probe）で復旧を確かめて閉じた回数です。 |
| `heterolog_failure_records_total` | Counter | `backend`・`action` | 送れなかったrecordを、その後の扱いごとに数えます。`spooled`（spoolへ保存）・`archived`（failure archiveへ保存）・`degraded`（stdoutへ退避）・`dropped`（過負荷時の方針 `drop` で破棄）・`failed`（保存も退避もできなかった）です。複数の保存方法を設定した場合は、それぞれが数えます。 |
| `heterolog_async_backend_failures_total` | Counter | `backend`・`reason`（`error`・`timeout`・`panic`） | 非同期API（`NewAsync`）が出力先へ渡したentryのうち、出力先がエラーを返したもの、`Timeout` までに終わらなかったもの、panicしたものの数です。非同期APIは、この失敗を呼出し元へ返しません。出力先が過負荷時の方針で破棄したentryは `error` です。 |
| `heterolog_async_rejected_entries_total` | Counter | `reason`（`invalid`・`closed`） | 非同期APIが、どの出力先にも渡さずに捨てたentryの数です。`invalid` は不正なentry（`MaxLogEntryBytes` を超えるものなど）、`closed` は `Close` を始めた後のentryです。 |
| `heterolog_object_storage_uploads_total` | Counter | `target`（`s3` など）・`outcome` | 圧縮したログファイルやfailure archiveなどのファイルを、object storageの1つの宛先へuploadした回数を、結果ごとに数えます。結果は `success`・`retryable`（残しておき、後でまたuploadします）・`permanent` です。spoolからの再送も含みます。 |
| `heterolog_object_storage_upload_duration_seconds` | Histogram | `target`・`outcome` | 1つの宛先への1回のuploadにかかった時間です。 |
| `heterolog_memory_pressure_ratio` | Gauge | `backend` | `UseMemoryPressureForOverload` のために出力先のspoolが起動したmemory逼迫の監視が、最後に読んだmemoryの使用率（0から1）です。信頼できる値を読めた場合だけ記録します。`WithMemoryPressureMonitor` で渡した監視は呼出し元のものなので、記録しません。 |
| `heterolog_memory_pressure_critical` | Gauge | `backend` | 同じ監視の最後の判定で、memoryが逼迫している（critical）なら1、そうでなければ0です。 |
| `heterolog_spool_replay_records_total` | Counter | `backend`・`outcome` | spoolから読み戻して送り直したrecordの数を、結果ごとに数えます。結果は `success`・`retryable`（spoolへ戻し、後でまた送り直します）・`dead`・`quarantine` です。応答をrecordへ対応付けられない場合はfile全体を隔離し、そのrecordをすべて `quarantine` として数えます。 |
| `heterolog_file_dropped_entries_total` | Counter | なし | fileロガーが過負荷時の方針 `drop` で破棄したentryの数です。 |
| `heterolog_internal_logs_total` | Counter | `level`（`error`・`warn`・`info`・`debug`） | 内部ログの通知先（`OnErrorConfig` など）へ渡した内部ログ（書き込みの失敗や遅いリクエストなど）の数を、levelごとに数えます。[内部ログの通知](#内部ログの通知)を参照してください。 |

### span

- span名は `heterolog.` で始まり、出力先（または `spool`・`failurearchive` などのロガーの部品）と操作が続きます。spanの属性も `heterolog.` で始まります。
- 操作が失敗した場合、spanはエラーを記録します。
- spanは、操作のcontextにあるspanの子になります。ログ呼出しが始めた送信（bufferが閾値に達した場合など）は、呼出し元のtraceに含まれます。
- fileロガーは、書き込みごとのspanを作りません。書き込みは `heterolog_backend_records_total` が数えます。

| span | 操作 | 属性 |
|---|---|---|
| `heterolog.objectstorage.upload` | 1つのファイルを、object storageの1つの宛先へuploadする処理です。 | `heterolog.target`・`heterolog.destination`（`s3-<SHA-256>` などの宛先ID） |
| `heterolog.spool.replay` | spoolから読み戻したrecordを1回送り直す処理です。retry clientの試行は、このspanの子になります。 | `heterolog.backend`・`heterolog.records` |
| `heterolog.<出力先>.create` | 出力先（`file`・`stdout`・`slack`・`opensearch`・`loki`・`cloudwatchlogs`・`clickhouse`）のロガーの構築です。 | |
| `heterolog.opensearch.flush`・`heterolog.loki.flush`・`heterolog.cloudwatchlogs.flush`・`heterolog.clickhouse.flush`・`heterolog.slack.send` | bufferからまとめて送る1回分の送信です。 | `heterolog.records`・`heterolog.bytes`（Slackにはありません） |
| `heterolog.opensearch.bulk`・`heterolog.opensearch.index`・`heterolog.loki.push`・`heterolog.cloudwatchlogs.put_log_events`・`heterolog.clickhouse.insert`・`heterolog.slack.post` | 1回のリクエストです。retry clientの再試行を含みます。 | `heterolog.records` |
| リクエストの名前に `.attempt` を付けた名前（`heterolog.loki.push.attempt` など） | retry clientの1回の試行です。その前の待ち時間は含みません。 | `heterolog.attempt`（初回は1） |
| `heterolog.file.compress`・`heterolog.file.purge.age`・`heterolog.file.purge.generation`・`heterolog.file.purge.dirsize` | ローテーションしたログファイルの圧縮と、期間・世代数・ディレクトリの容量によるログファイルの削除です。 | `heterolog.compress.format` |
| `heterolog.failurearchive.write`・`heterolog.failurearchive.write.retryable`・`heterolog.failurearchive.write.permanent`・`heterolog.failurearchive.compress`・`heterolog.failurearchive.purge.age`・`heterolog.failurearchive.purge.generation`・`heterolog.failurearchive.purge.dirsize`・`heterolog.failurearchive.delegate`・`heterolog.failurearchive.create` | 送れなかったrecordのfailure archiveへの保存、そのファイルの保守、内部ログの通知、構築です。 | |
| `heterolog.fallback.slack.create` | 内部ログの通知に使うSlackロガーの構築です。 | |
| `heterolog.internal_log.notify` | 1件の内部ログを、1つの通知先へ渡す処理です。 | `heterolog.destination`（`file`・`slack` など）、`heterolog.level` |

操作のspanに足すevent：

- `heterolog.<出力先>.retry_decision`（`opensearch`・`loki`・`cloudwatchlogs`・`clickhouse`）：試行が失敗した後のretry clientの再試行の判断（理由・試行の回数・HTTPステータス）と、circuit breakerが失敗を記録したときやリクエストを断ったときの判断です。
- `heterolog.failure.overload.dropped`：過負荷時の方針 `drop` で破棄した、送れなかったrecordです。失敗の分類ごとのrecordの数を持ちます。spanには属性 `heterolog.failure.overload.drop_occurred` も付きます。
- `heterolog.file.overload.dropped`：fileロガーが過負荷時の方針 `drop` で破棄したentryです。

## 出力先ごとの注意

### Slack

- `MaxRestThreshold` はログ到着の休止時間です。この時間を超えてログが届くと、待機中のログとともに即時送信を予約します。送信中の通知がある場合は、その完了を待ちます。
- `pkg/logger` の `SlackLoggerConfig`・`DeliverySlackLoggerConfig` の `HTTPClient` には、Slack APIの呼び出しに使う `*http.Client`（プロキシや独自のtransportを使う場合など）を指定できます。nilの場合は `net/http` の既定のtransportを使います。
  - 指定したクライアントはロガーの構築時にコピーし、`Transport`・`Jar` を引き継ぎます。元のクライアントは変更せず、構築後に変更しても反映しません。リダイレクトは、`CheckRedirect` の設定にかかわらず追従しません（[送信先URLとリダイレクト](#送信先urlとリダイレクト)）。
  - タイムアウトには各設定の `Timeout`（正の値が必須）を使い、元のクライアントの `Timeout` より優先します。
  - 同じSlack設定を使う内部ログの送信にも、同じクライアントを使います。
  - 環境設定からは指定できないため、環境設定で有効にしたSlack出力は既定のtransportを使います。
- Slackバックエンドは、配送用contextが送信開始前に終了した場合を `DeliveryRejected`、送信後の通信障害や不完全な応答を `DeliveryUnconfirmed` として返します。
- 公開Delivery APIは、呼び出し元のキャンセルから独立した配送期限を使います。
- 成功判定には応答本文全体の検証を行います。
- Slackには失敗ログの永続保存・quarantine機能がないため、Deliveryの成否不明ログを保存する場合は呼び出し側で扱います。
- 通常・非同期のSlack送信では、応答本文の検証に失敗して成否を確認できないバッチも再送します。欠落より重複を優先するため、同じ通知が重複して投稿される場合があります。

### CloudWatch Logs

- stream分割後の名前は、実際に使う通常出力・内部ログの設定を検証します。出力先として選択していないbase設定や、内部ログ専用設定で置き換えたbase設定は検証対象から除きます。
- 容量制限はJSON送信後のメッセージ長とイベントごとの付加サイズ（26バイト）で判定し、1件のメッセージは1,048,550バイトまで送信します。超えたログは、同じ送信単位の他のログを止めずに、そのログだけを送信対象から除外します。
- 送信先URLを省略すると、リージョンに応じた標準エンドポイントを使います。中国リージョンでは `amazonaws.com.cn` を使い、URLを明示した場合はその指定を優先します。
- ストリーム自動作成では、ログ送信が未作成ストリームへの送信として拒否された後、作成中にキャンセルされても、未受理のログは再送対象として保存します。作成APIの恒久的な拒否は配送不能として扱い、作成後にログを再送して結果が不明になった場合はquarantineへ隔離します。
- 認証情報取得や署名で失敗し、ログのHTTP要求が未送信と確認できる場合も、再送対象として保持します。元の原因は `errors.Is`・`errors.As` で取得できます。

### Loki

- 行上限は、送信後の文字列のUTF-8バイト数で判定します。不正なUTF-8はGoのJSON変換に従って置換され、その置換後の長さで分割・除外を行います。

### OpenSearch

- 単一文書送信では、インデックス名を一つのURLパス要素としてエンコードします。`<logs-{now/d}>` の日付計算式やタイムゾーン指定に含まれる特殊文字も保持します。

### 送信先URLとリダイレクト

- LokiとCloudWatch Logsの送信先URLは、末尾の通常の `/` を除く際にも `%2F` などのエスケープ表現を保持します。エンコードされた `/` をパス区切りに変換しません。
- どの出力先もリダイレクトを追従しません。Loki・ClickHouse・OpenSearch・CloudWatch Logs・Slack・VictoriaMetricsへの送信とS3へのアップロードは、各リクエストを一回だけ送信し、301・302・303・307・308を含むリダイレクトをHTTPステータスのエラーとして返します。追従すると、リクエストが本文なしのGETに変わるか、設定していないURLへログを送るためです。渡したHTTPクライアントのリダイレクト設定は使わず、変更もしません。
- リダイレクトは、宛先・設定の誤りで配送不能が確定した失敗（permanent）として扱い、自動再送しません。たとえば圧縮ログは、fileArchiveの `permanent`・spoolの `dead` に保持します。S3がbucketのDNS更新中に返す `307 TemporaryRedirect` だけは、一時的な失敗として再送します。

### ClickHouse

- `pkg/clickhouse` の `InsertDeduplicationToken` は、前後の空白を含めてそのまま送信します。空文字だけが未指定扱いです。同じトークンは再送を同一INSERTとして扱わせるために使い、別のINSERTには別の値を指定してください。

### S3とobject storage

- `pkg/objectstorage/uploader.NewUploadInput` の保存名は前後の空白も含めて保持し、空文字だけを拒否します。
- S3へのアップロードは空ファイルにも対応します。
- S3のリージョンは前後の空白だけを除いた値で署名し、大文字小文字は変更しません。宛先IDでは、大文字小文字だけが異なるリージョンを同じ保存先として扱います。
- S3のエンドポイントにはクエリ（`?` だけの空クエリを含む）とフラグメントを指定できません。
- S3互換のサービスも、AWS Signature Version 4で本文に署名しない形を受け付けるなら、同じ `HETEROLOG_OBJECT_STORAGE_S3_*` の設定で使えます。アップロードは、`X-Amz-Content-Sha256: UNSIGNED-PAYLOAD` で署名したPUTです。`HETEROLOG_OBJECT_STORAGE_S3_ENDPOINT` にはそのサービスのエンドポイントを、`HETEROLOG_OBJECT_STORAGE_S3_REGION` にはそのサービスが署名に求めるリージョンを指定します。
- Backblaze B2はこの方法で使えます。エンドポイントには、bucketの画面に表示されるS3のエンドポイントに `https://` を付けて指定し、リージョンにはその名前に含まれるもの（`us-west-004` など）を指定します。アクセスキーIDには、そのbucketに書き込めるapplication keyのkeyIDを、シークレットアクセスキーにはそのapplicationKeyを指定し、`HETEROLOG_OBJECT_STORAGE_S3_SESSION_TOKEN` は空にします。master application keyは、B2のS3互換APIでは使えません。virtual-hosted-styleとpath-styleのどちらでも送れるため、`HETEROLOG_ENABLE_OBJECT_STORAGE_S3_FORCE_PATH_STYLE` は `false` のままで構いません。bucketは、アップロードの用途ごとの `*_OBJECT_STORAGE_S3_BUCKET` に指定します。例:

  ```sh
  HETEROLOG_OBJECT_STORAGE_S3_ENDPOINT=https://s3.us-west-004.backblazeb2.com
  HETEROLOG_OBJECT_STORAGE_S3_REGION=us-west-004
  HETEROLOG_OBJECT_STORAGE_S3_ACCESS_KEY_ID=<keyID>
  HETEROLOG_OBJECT_STORAGE_S3_SECRET_ACCESS_KEY=<applicationKey>
  HETEROLOG_LOG_FILE_OBJECT_STORAGE_S3_BUCKET=<bucket>
  ```

### VictoriaMetrics

- `pkg/victoriametrics` の `SendAsync` は、受理するメトリクスとラベルを複製します。呼び出しから戻った後は入力を再利用できます。
- `WithHTTPClient` に渡したクライアントの設定は変更せず、同期・非同期とも各HTTP要求に `WithTimeout` を適用します。再送にも毎回同じ期限を使い、0は期限なしを表します。

## ログ復旧CLI

ライブラリに加えて、停止中のアプリケーションのログファイルを復旧するCLIを同梱しています。

```sh
go build -o log-recovery ./cmd/log-recovery
./log-recovery --help
./log-recovery --target my-service --application-stopped --env-file /path/to/logging.env
```

- `--target` はログ生成時と同じ識別子にします。英数字で始まる英数字・ハイフン・アンダースコア・ドットを使用でき、パス区切りやWindowsの予約名は使用できません。
- CLIの環境設定項目は [.env.example](../.env.example)、復旧対象や操作手順は [手動ログ復旧](manual-log-recovery.txt) を参照してください。
- CLIは明示した環境設定を読み込みます。読み込んだ設定に `--target` を反映した後、アプリケーションの設定全体の検証（保存先ディレクトリの重複と所有権、upload対象、failure storeの構成、timeout階層（`must_fit` など）、memory healthの閾値）を行います。不整合がある場合は、復旧処理を始める前に設定エラー（`invalid config: ...`、終了コード1）で終了し、保存先のディレクトリやファイルを作成・変更しません。
- 認証情報を含み得る値（`*_HEADERS` のHTTP header、`*_S3_DESTINATIONS` の一覧）が不正な場合、設定エラーはkeyと種類だけを示し、値は表示文にも `errors.As` で取り出せるerrorにも含めません。
- 環境設定で `HETEROLOG_LOG_TYPE`・`HETEROLOG_ASYNC_LOG_TYPE`・`HETEROLOG_DELIVERY_LOG_TYPE` を省略または空にした場合、出力先は通常・非同期・Deliveryとも標準出力だけです。fileやSlackなどは明示した場合だけ有効になり、Slackのトークンなど必要な認証情報がなければ設定エラーになります。
- fileのログを復旧する場合は、ログ生成時と同じ出力先（例: `HETEROLOG_LOG_TYPE=file`）を指定してください。
- 環境設定から読み込む場合も、`HETEROLOG_ENABLE_LOCAL_TIME_LOG` を指定しなければUTCで記録します。
- spoolの `dead`・`quarantine` と、圧縮ログupload失敗の保存先（`retryable`・`permanent`・`dead`・`quarantine`）は、環境設定で削除を有効にしない限り自動削除しません。
- backend送信失敗のfileArchive（`retryable`・`permanent`）も、既定では保持世代数・保持日数・ディレクトリ容量の上限（`HETEROLOG_MAX_*_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS`・`HETEROLOG_MAX_*_ENABLE_DURABLE_FILE_ARCHIVE_AGE`・`HETEROLOG_MAX_*_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB`）がすべて0（上限なし）で、自動削除しません。0以外を指定した条件だけで古いファイルを削除し、指定できる値は世代数2以上・日数1以上・容量10MB以上かつファイル最大サイズ以上です。
- コードから設定する `durable.FileArchiveConfig` の `MaxFileBackups`・`MaxFileAge`・`MaxDirSize` も、0は上限なしです。以前の既定（60世代・60日・2048MB）で自動削除を続ける場合は、これらの値を明示してください。
- 上限なしでもディスクを使い切らないよう、空き容量が `HETEROLOG_MIN_AVAILABLE_DISK_SPACE_GB` を下回る間は新しい失敗をfileArchiveへ保存せず、`*_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY`（`drop`=破棄、`degrade`=標準出力へ退避）に従って処理します。fileArchiveを使う場合、この閾値は0にできません。

## テスト

```sh
go test ./...
go vet ./...
go test -race . ./internal/application/logging ./internal/application/loggerruntime ./internal/bootstrap ./internal/presentation/sloghandler ./internal/infrastructure/logger ./internal/infrastructure/logger/multi
```

- 通常のテストは、公開設定例から作るダミー設定とローカルHTTPサーバーを使い、開発者の `.env` を必要としません。
- 設定例の値のままfile loggerの出力を確かめる試験は、一時ディレクトリのファイルシステムに `HETEROLOG_MIN_AVAILABLE_DISK_SPACE_GB`（設定例では1GB）以上の空き容量を必要とし、不足する場合は空き容量の不足を示して失敗します。
- 実サービスに接続する結合テストは、個別の環境変数で明示的に有効化します。
- [testenv/windows](../testenv/windows/README.md) には、Windows/macOS/LinuxのCIと、CIの `go test ./...` と同じ全packageをWindows VMで実行する手順を含む、Windows固有の追加試験手順を用意しています。
- [testenv/macos](../testenv/macos/README.md) には、macOSの実機で、大小文字を区別するAPFS・ExFAT・FAT32のdisk imageを一時ディレクトリにする試験と、Docker上の結合試験を実行する手順を用意しています。
- CIのLinuxでは、macOSと同じく一時ディレクトリがsymlinkを経由する条件（`TMPDIR` をsymlinkの下に置く）でも全体を試験します。

Linuxの大文字小文字を区別しないファイルシステム:

- Linuxで大文字小文字を区別しないファイルシステム（ext4・F2FS・tmpfsのcasefold、FAT）の判定の処理そのものは、ファイルシステムの種類・casefoldの属性・mount optionを差し替える試験で毎回確かめています。
- 実際のcasefoldディレクトリやFATのmountで判定を確かめる試験（`pkg/path/pathidentity` の `TestLinuxDirectoryCaseDetectsAvailableCaseInsensitiveFilesystems`）と、一時ディレクトリが大文字小文字を区別しない場合だけ動くfile loggerの試験は、それらを用意できない環境ではskipします。
- CIの `test (ubuntu-latest, case-insensitive TMPDIR)` jobは、loop mountしたext4のcasefoldディレクトリを一時ディレクトリにして全テストを実行し、vfat（runnerが対応していればexFATも）のmountの判定と、vfat上の一時ディレクトリでのfile loggerの試験も実行します。このjobでは、これらの試験がskipされると失敗にします。

実サービスとの結合テスト:

- 試験用の宛先と認証情報を書いた専用の環境設定ファイルを用意し、試験ごとのopt-in変数とともに `HETEROLOG_INTEGRATION_ENV_PATH` で指定します。`go test` は各packageのディレクトリで試験を実行するため、絶対パスで指定してください。
- `pkg/opensearch` の試験も、プロセスの環境変数ではなくこのファイルだけから読みます。接続先の `HETEROLOG_OPENSEARCH_ENDPOINT`、認証方式の `HETEROLOG_OPENSEARCH_AUTH_TYPE`（認証なしは `none`、Basic認証では `basic` と `HETEROLOG_OPENSEARCH_USERNAME` / `HETEROLOG_OPENSEARCH_PASSWORD`）、書き込むindexの `OPENSEARCH_TEST_INDEX`（`app-log` で始まる名前。省略時は `app-log-test-index`）です。
- 使い捨てのコンテナに対してすべての結合テストを実行するCI用の設定と手順は、[testenv/integration](../testenv/integration/README.md) にあります。

```sh
OPENSEARCH_INTEGRATION_TEST=true HETEROLOG_INTEGRATION_ENV_PATH=/path/to/integration.env \
  go test ./pkg/opensearch -count=1
```

- 結合テストは既定でTLS証明書を検証します。自己署名証明書を使う試験環境だけ、専用ファイルに `HETEROLOG_OPENSEARCH_SKIP_VERIFY_SSL=true` を指定してください。`.env.example` ではこの値が `true` のため、`.env.example` を基に専用ファイルを作り証明書を検証する場合は `false` にしてください。
- loggerの結合テストも同じ専用ファイルを使います。`.env.example` を基に試験用ファイルを用意し、対象の `HETEROLOG_OPENSEARCH_ENDPOINT`、`HETEROLOG_LOKI_ENDPOINT` または `HETEROLOG_CLICKHOUSE_ENDPOINT` を明示します。
- 試験はERRORログのsampling率を1へ上書きするため、専用ファイルのsampling設定は結果に影響しません。
- OpenSearchのBasic認証では、専用ファイルに `HETEROLOG_OPENSEARCH_USERNAME` / `HETEROLOG_OPENSEARCH_PASSWORD` を指定します。認証情報は補完しないため、未指定のままOpenSearchを使うと設定エラーになります。認証なしの接続先では `HETEROLOG_OPENSEARCH_AUTH_TYPE=none` を指定してください。

```sh
OPENSEARCH_INTEGRATION_TEST=true HETEROLOG_INTEGRATION_ENV_PATH=/path/to/integration.env \
  go test ./internal/infrastructure/logger -count=1
```

- Loki・ClickHouseはそれぞれ `LOKI_INTEGRATION_TEST=true`・`CLICKHOUSE_INTEGRATION_TEST=true` で有効化します。file loggerのTempo・OpenTelemetry Collectorの試験は、`TEMPO_INTEGRATION_TEST=true`・`OTEL_COLLECTOR_INTEGRATION_TEST=true` と同じ専用ファイルを使用します。`HETEROLOG_ENV_PATH` だけでは実サービスの試験は開始しません。
- 結合テストの実行中は、専用ファイルにない本ライブラリの設定項目（`.env.example` の項目、`HETEROLOG_ENV_PATH`、既定値として参照する `AWS_ACCESS_KEY_ID`・`AWS_SECRET_ACCESS_KEY`・`AWS_SESSION_TOKEN`・`AWS_REGION`）をプロセスの環境変数から外します。`PATH`、`HOME`、`TMP`／`TEMP`／`TMPDIR`、`SystemRoot`、プロキシや証明書の設定などの実行環境はそのまま使います。
