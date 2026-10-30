# 実serviceの結合テスト（CI・ローカル）

opt-inの結合テスト（実serviceへ接続する試験）を、使い捨てのlocal containerに対して実行するための
設定とscriptです。GitHub Actionsのjob `integration (ubuntu-latest, live backends)` と同じ手順を
ローカルでも実行できます。dockerが必要で、初回はimageをdownloadします（OpenSearchなど大きいimageを含みます）。

宛先は127.0.0.1だけで、secretや共有環境の認証情報は使いません。containerのdataは
containerと一緒に破棄します。

## 構成

| file | 役割 |
| --- | --- |
| `ci.env` | CI専用の環境設定file。`HETEROLOG_INTEGRATION_ENV_PATH` に指定します |
| `start-services.sh` | containerを起動し、準備完了まで待ちます。失敗時は診断情報を表示します |
| `run-live-tests.sh` | groupごとに対象の試験だけを実行し、すべてPASSしたかを確認します |
| `collect-diagnostics.sh` | containerの状態・log・準備完了の確認先の応答を表示します |
| `stop-services.sh` | 起動したcontainerとnetworkを削除します |
| `common.sh` | image tag・container名などの共通定義です |
| `tempo.yaml`・`otel-collector.yaml` | TempoとOpenTelemetry Collectorの設定です |

## 対象の試験

| group | opt-inの変数 | package | 試験 |
| --- | --- | --- | --- |
| `opensearch` | `OPENSEARCH_INTEGRATION_TEST` | `internal/infrastructure/logger`、`internal/bootstrap` | `TestOpenSearchLoggerUsesActualDotEnvConfigAndStoresDocument`、`TestOpenSearchSpoolUsesActualDotEnvConfigActiveAndResendsToBackend`、`TestTelemetryWritesSpansToOpenSearch`、`TestTelemetryResendsSpooledSpansToOpenSearch` |
| `opensearch-client` | `OPENSEARCH_INTEGRATION_TEST` | `pkg/opensearch` | `TestIndexDocument(s)ToLocalOpenSearch`、`TestIndexDocument(s)CompressedToLocalOpenSearch`（4件） |
| `loki` | `LOKI_INTEGRATION_TEST` | `internal/infrastructure/logger`、`internal/infrastructure/logger/loki` | `TestLokiLoggerUsesActualDotEnvConfigAndStoresEntry`、`TestLokiSpoolUsesActualDotEnvConfigActiveAndResendsToBackend`、`TestLokiLoggerWithIntegrationEnvironmentPushesToLocalLoki` |
| `clickhouse` | `CLICKHOUSE_INTEGRATION_TEST` | `internal/infrastructure/logger`、`internal/bootstrap` | `TestClickHouseLoggerUsesActualDotEnvConfigAndStoresRow`、`TestClickHouseSpoolUsesActualDotEnvConfigActiveAndResendsToBackend`、`TestTelemetryInsertsSpansIntoClickHouse`、`TestTelemetryResendsSpooledSpansToClickHouse` |
| `tracing` | `TEMPO_INTEGRATION_TEST`、`OTEL_COLLECTOR_INTEGRATION_TEST` | `internal/infrastructure/logger/file`、`internal/bootstrap` | `TestFileLogger_Log_WritesToFile_ForTempo`、`TestFileLogger_Log_WritesToFile_ForTempo_Via_Collector`、`TestTelemetrySendsSpansToTempoOverOTLPHTTP`、`TestTelemetryResendsSpooledSpansToTempo` |
| `victoriametrics` | `VICTORIAMETRICS_INTEGRATION_TEST` | `internal/bootstrap` | `TestTelemetrySendsMetricsToVictoriaMetricsOverOTLP`、`TestTelemetrySendsMetricsToVictoriaMetricsInThePrometheusFormat`、`TestTelemetryResendsSpooledMetricsToVictoriaMetrics` |

`run-live-tests.sh` は、選んだ試験のうちPASSしなかったもの（失敗のほか、skipや試験名の変更で
実行されなかったもの）があれば失敗します。試験を追加・改名した場合は、scriptの一覧も更新してください。

`internal/testutil` の `TESTUTIL_LIVE_TEST` は、opt-inしなければskipすることを確かめる試験です。
実serviceを使わず、通常の `go test ./...` で実行します。CloudWatch Logs・Slack・S3には
実serviceの結合テストがありません。

## container

| service | image | 公開先（127.0.0.1） | 準備完了の確認 |
| --- | --- | --- | --- |
| OpenSearch | `opensearchproject/opensearch:2.19.1` | 29200 | `/_cluster/health` がyellow以上 |
| Loki | `grafana/loki:3.4.2` | 23100 | `/ready` |
| ClickHouse | `clickhouse/clickhouse-server:24.8.4.13` | 28123 | 認証付きの `SELECT 1` |
| Tempo | `grafana/tempo:2.7.2` | 24317（OTLP gRPC）、24318（OTLP HTTP）、23200（HTTP） | `/ready` |
| OpenTelemetry Collector | `otel/opentelemetry-collector-contrib:0.120.0` | 25317（OTLP gRPC）、23133（health check） | health check extension |
| VictoriaMetrics | `victoriametrics/victoria-metrics:v1.106.1` | 28428（OTLP HTTP・Prometheusのtext形式の取り込み・HTTP API） | `/health` |

- 開発者がローカルで既定port（9200・3100・8123・4317など）のserviceを動かしていても書き込まない
  よう、CI専用のport帯を使います。使用中のportがあれば、起動前に失敗します。
- OpenSearchはsecurity pluginを無効にして起動し、`HETEROLOG_OPENSEARCH_AUTH_TYPE=none` で接続します。
  暗黙の管理者認証情報は使いません。
- ClickHouseは `ci.env` と同じ使い捨ての利用者で起動します（`default` 利用者は使いません）。
- Lokiはimage同梱の単一node設定で起動します。Collectorは受信したspanを同じdocker networkの
  Tempoへ転送します。TempoとLokiは利用状況の送信を止めています。
- imageの版は固定しています（`latest` は使いません）。更新する場合は `common.sh` とこの表を変更します。

## CIでの実行

`.github/workflows/test.yml` の `integration (ubuntu-latest, live backends)` が、
`start-services.sh` でcontainerを起動し、groupごとのstepで `run-live-tests.sh` を実行します。
前のgroupが失敗しても残りのgroupを実行し、試験が失敗した場合は `collect-diagnostics.sh` で
containerのlogを表示します。最後に `stop-services.sh` で必ず片付けます。

## ローカルでの実行

docker、curl、bash、Goが必要です。repositoryのrootで実行します。

```sh
testenv/integration/start-services.sh
testenv/integration/run-live-tests.sh              # 全group
testenv/integration/run-live-tests.sh loki tracing # 一部のgroup
testenv/integration/stop-services.sh
```

`go test` を直接実行する場合は、opt-inの変数と `HETEROLOG_INTEGRATION_ENV_PATH` を指定します。
`go test` は各packageのdirectoryで試験を実行するため、`HETEROLOG_INTEGRATION_ENV_PATH` は
絶対pathで指定してください（`run-live-tests.sh` は相対pathを絶対pathへ直します）。

```sh
LOKI_INTEGRATION_TEST=true HETEROLOG_INTEGRATION_ENV_PATH="$PWD/testenv/integration/ci.env" \
  go test -count=1 -v -run '^TestLokiLoggerWithIntegrationEnvironmentPushesToLocalLoki$' \
  ./internal/infrastructure/logger/loki
```

どの試験も、接続先・認証情報などの設定は専用fileだけから読みます。試験中は本ライブラリの
設定項目をprocessの環境変数から外すため、shellでexportした設定値は使いません（opt-inの変数と
`HETEROLOG_INTEGRATION_ENV_PATH` はshellで指定します）。`pkg/opensearch` の試験も、`HETEROLOG_OPENSEARCH_ENDPOINT`・`HETEROLOG_OPENSEARCH_AUTH_TYPE`（Basic認証では `HETEROLOG_OPENSEARCH_USERNAME`・
`HETEROLOG_OPENSEARCH_PASSWORD`）と書き込むindexの `OPENSEARCH_TEST_INDEX` を専用fileから読みます。

## `ci.env` の要点

- `HETEROLOG_LOG_TYPE` は指定しません（既定の標準出力）。backendの試験は、試験対象のbackendで
  `HETEROLOG_LOG_TYPE` を上書きします。
- `HETEROLOG_LOG_TIMEOUT_SECOND=30`・`HETEROLOG_OPENSEARCH_TIMEOUT_SECOND=2`: 既定のtimeout budget policy
  （`must_fit`）では、loggerのtimeoutがclientのretry予算を上回る必要があります。既定値
  （logger 18s、OpenSearch 1s、Loki・ClickHouse 2s）のままでも検証は通りますが、起動直後の
  containerに合わせてOpenSearchのclient timeoutを2sにするため、その予算（26s）を上回る
  30sを指定しています。
- Tempo試験の送信先は `HETEROLOG_OTEL_TEMPO_ADDR`、Collector試験の送信先は `HETEROLOG_OTEL_COLLECTOR_ADDR` です。
  `NewTelemetry` の試験は、`HETEROLOG_TRACES_OTLP_ENDPOINT` へOTLP/HTTPでspanを送り、`HETEROLOG_OTEL_TEMPO_QUERY_ADDR`
  （TempoのHTTP API）でtraceを読み戻して、届いたことを確かめます。
  metricsの試験は、`HETEROLOG_METRICS_OTLP_ENDPOINT`（VictoriaMetricsのOTLP/HTTP）と
  `HETEROLOG_METRICS_VICTORIAMETRICS_ENDPOINT`（Prometheusのtext形式の取り込み）へmetricsを送り、
  `HETEROLOG_VICTORIAMETRICS_QUERY_ADDR`（VictoriaMetricsのHTTP API）で読み戻します。
  どの試験も、専用の設定fileに送信先のkeyがない場合は既定の送信先へ送らずに失敗します。
- portを変える場合は `ci.env` の宛先を変更します。`start-services.sh` は `ci.env` の宛先から
  公開するportを決めます（Collectorのhealth checkのportは `common.sh` で定義）。
  別の環境設定fileを使う場合も `HETEROLOG_INTEGRATION_ENV_PATH` で指定できますが、宛先は
  127.0.0.1にしてください。`start-services.sh` は127.0.0.1以外の宛先を拒否しますが、
  `run-live-tests.sh` は宛先を確かめずにfileの値へ送信します。

## 失敗したとき

- `start-services.sh` は、準備完了を待つ間にcontainerが停止した場合や時間内に準備できなかった
  場合、`collect-diagnostics.sh` の結果を表示して失敗します。containerは調査できるよう残すため、
  `stop-services.sh` で削除してください。
- 表示するlogの行数は `IT_DIAGNOSTICS_LOG_LINES`（既定200）で変更できます。
- `docker pull` は一時的な失敗に備えて3回まで試します。tagが存在しない場合と取得回数の上限
  （Docker Hubのrate limit）に達した場合は再試行せずに失敗するため、image tagとnetworkの接続を
  確認してください。
