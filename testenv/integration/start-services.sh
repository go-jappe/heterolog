#!/usr/bin/env bash
# 結合テスト用の使い捨てcontainer（OpenSearch・Loki・ClickHouse・Tempo・OpenTelemetry Collector）を
# 起動し、準備完了まで待ちます。宛先のportとClickHouseの認証情報は環境設定file
# （HETEROLOG_INTEGRATION_ENV_PATH、未指定なら同じdirectoryのci.env）から読みます。
# 前回の実行で残ったcontainerは作り直します。準備できない場合は診断情報を表示して失敗します。
# imageが手元にない場合はdocker pullで取得します。
set -euo pipefail

# shellcheck source=testenv/integration/common.sh
. "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

containers_created=

on_exit() {
	local status=$?
	if ((status != 0)) && [[ -n $containers_created ]]; then
		it_error "backend services did not become ready; diagnostics follow"
		bash "$IT_DIR/collect-diagnostics.sh" || true
	fi
	exit "$status"
}
trap on_exit EXIT

it_require_commands docker curl
it_log "environment file: $IT_ENV_FILE"

it_loopback_port HETEROLOG_OPENSEARCH_ENDPOINT
opensearch_port=$IT_PORT
it_read_env HETEROLOG_OPENSEARCH_AUTH_TYPE
if [[ $IT_VALUE != none ]]; then
	it_error "HETEROLOG_OPENSEARCH_AUTH_TYPE must be none because the OpenSearch container runs without the security plugin"
	exit 1
fi
it_loopback_port HETEROLOG_LOKI_ENDPOINT
loki_port=$IT_PORT
it_loopback_port HETEROLOG_CLICKHOUSE_ENDPOINT
clickhouse_port=$IT_PORT
it_read_env HETEROLOG_CLICKHOUSE_USERNAME
clickhouse_user=$IT_VALUE
it_read_env HETEROLOG_CLICKHOUSE_PASSWORD
clickhouse_password=$IT_VALUE
it_loopback_port HETEROLOG_OTEL_TEMPO_ADDR
tempo_otlp_port=$IT_PORT
it_loopback_port HETEROLOG_OTEL_COLLECTOR_ADDR
collector_otlp_port=$IT_PORT

# containerのuser（Tempo・Collectorは非root）が設定fileを読めるか、複製前に確認します。
for config in tempo.yaml otel-collector.yaml; do
	if [[ ! -f $IT_DIR/$config || -z $(find "$IT_DIR/$config" -perm -0004 -print) ]]; then
		it_error "$IT_DIR/$config must be world-readable (chmod a+r)"
		exit 1
	fi
done

it_log "removing containers left by a previous run"
it_remove_services

for port in "$opensearch_port" "$loki_port" "$clickhouse_port" "$tempo_otlp_port" \
	"$collector_otlp_port" "$IT_TEMPO_HTTP_PORT" "$IT_OTEL_COLLECTOR_HEALTH_PORT"; do
	it_require_free_port "$port"
done

it_group "pulling images"
for image in "$IT_OPENSEARCH_IMAGE" "$IT_LOKI_IMAGE" "$IT_CLICKHOUSE_IMAGE" \
	"$IT_TEMPO_IMAGE" "$IT_OTEL_COLLECTOR_IMAGE"; do
	it_pull_image "$image"
done
it_endgroup

containers_created=1
docker network create --label "$IT_LABEL" "$IT_NETWORK" >/dev/null

# 各containerは127.0.0.1だけで公開します。OpenSearchはsecurity pluginを無効にし、
# 管理者passwordを設定せずに認証なしで起動します。
it_log "starting $IT_OPENSEARCH_IMAGE"
docker run -d --name "$IT_OPENSEARCH_CONTAINER" --label "$IT_LABEL" --network "$IT_NETWORK" \
	-p "127.0.0.1:$opensearch_port:9200" \
	-e discovery.type=single-node \
	-e DISABLE_SECURITY_PLUGIN=true \
	-e DISABLE_INSTALL_DEMO_CONFIG=true \
	-e DISABLE_PERFORMANCE_ANALYZER_AGENT_CLI=true \
	-e "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m" \
	"$IT_OPENSEARCH_IMAGE" >/dev/null

# Lokiはimage同梱の単一node設定（認証なし・filesystem保存）で起動し、利用状況の送信だけを止めます。
it_log "starting $IT_LOKI_IMAGE"
docker run -d --name "$IT_LOKI_CONTAINER" --label "$IT_LABEL" --network "$IT_NETWORK" \
	-p "127.0.0.1:$loki_port:3100" \
	"$IT_LOKI_IMAGE" -config.file=/etc/loki/local-config.yaml -reporting.enabled=false >/dev/null

# ClickHouseは環境設定fileと同じ使い捨ての利用者を作成します（既定のdefault利用者は使いません）。
it_log "starting $IT_CLICKHOUSE_IMAGE"
docker run -d --name "$IT_CLICKHOUSE_CONTAINER" --label "$IT_LABEL" --network "$IT_NETWORK" \
	--ulimit nofile=262144:262144 \
	-p "127.0.0.1:$clickhouse_port:8123" \
	-e "CLICKHOUSE_USER=$clickhouse_user" \
	-e "CLICKHOUSE_PASSWORD=$clickhouse_password" \
	"$IT_CLICKHOUSE_IMAGE" >/dev/null

# Tempo・Collectorの設定fileはbind mountではなくdocker cpで渡します
# （SELinuxのlabelやremoteのdocker daemonに依存しません）。
it_log "starting $IT_TEMPO_IMAGE"
docker create --name "$IT_TEMPO_CONTAINER" --label "$IT_LABEL" --network "$IT_NETWORK" \
	--network-alias tempo \
	-p "127.0.0.1:$IT_TEMPO_HTTP_PORT:3200" \
	-p "127.0.0.1:$tempo_otlp_port:4317" \
	"$IT_TEMPO_IMAGE" -config.file=/etc/tempo.yaml >/dev/null
docker cp "$IT_DIR/tempo.yaml" "$IT_TEMPO_CONTAINER:/etc/tempo.yaml"
docker start "$IT_TEMPO_CONTAINER" >/dev/null

it_log "starting $IT_OTEL_COLLECTOR_IMAGE"
docker create --name "$IT_OTEL_COLLECTOR_CONTAINER" --label "$IT_LABEL" --network "$IT_NETWORK" \
	-p "127.0.0.1:$collector_otlp_port:4317" \
	-p "127.0.0.1:$IT_OTEL_COLLECTOR_HEALTH_PORT:13133" \
	"$IT_OTEL_COLLECTOR_IMAGE" --config=/etc/otelcol-contrib/config.yaml >/dev/null
docker cp "$IT_DIR/otel-collector.yaml" "$IT_OTEL_COLLECTOR_CONTAINER:/etc/otelcol-contrib/config.yaml"
docker start "$IT_OTEL_COLLECTOR_CONTAINER" >/dev/null

# 起動は並行して進むため、待機時間の上限は各serviceの起動にかかる時間の目安です。
it_wait_ready OpenSearch "$IT_OPENSEARCH_CONTAINER" \
	"http://127.0.0.1:$opensearch_port/_cluster/health?wait_for_status=yellow&timeout=5s" 240
it_wait_ready Loki "$IT_LOKI_CONTAINER" "http://127.0.0.1:$loki_port/ready" 180
# 認証付きのqueryで、利用者の作成まで終わったことを確認します。
it_wait_ready ClickHouse "$IT_CLICKHOUSE_CONTAINER" "http://127.0.0.1:$clickhouse_port/?query=SELECT%201" 180 \
	--user "$clickhouse_user:$clickhouse_password"
it_wait_ready Tempo "$IT_TEMPO_CONTAINER" "http://127.0.0.1:$IT_TEMPO_HTTP_PORT/ready" 180
it_wait_ready "OpenTelemetry Collector" "$IT_OTEL_COLLECTOR_CONTAINER" \
	"http://127.0.0.1:$IT_OTEL_COLLECTOR_HEALTH_PORT/" 120

it_log "all backend services are ready:"
it_log "  OpenSearch  http://127.0.0.1:$opensearch_port"
it_log "  Loki        http://127.0.0.1:$loki_port"
it_log "  ClickHouse  http://127.0.0.1:$clickhouse_port"
it_log "  Tempo       127.0.0.1:$tempo_otlp_port (OTLP gRPC), http://127.0.0.1:$IT_TEMPO_HTTP_PORT"
it_log "  Collector   127.0.0.1:$collector_otlp_port (OTLP gRPC)"
