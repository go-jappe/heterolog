#!/usr/bin/env bash
# 結合テスト用containerの状態、log、準備完了の確認先の応答を表示します。
# start-services.shが起動に失敗したときと、CIで試験が失敗したときに実行します。
# 診断は可能な範囲で続け、このscript自体は失敗しません。
# 表示するlogの行数はIT_DIAGNOSTICS_LOG_LINES（既定200）で変更できます。
set -euo pipefail

# shellcheck source=testenv/integration/common.sh
. "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

if ! it_require_commands docker; then
	exit 0
fi

log_lines=${IT_DIAGNOSTICS_LOG_LINES:-200}

it_group "integration containers"
docker ps -a --filter "label=$IT_LABEL" \
	--format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}' || true
it_endgroup

for container in "${IT_CONTAINERS[@]}"; do
	it_group "$container (last $log_lines log lines)"
	if docker container inspect "$container" >/dev/null 2>&1; then
		docker inspect -f 'status={{.State.Status}} exit_code={{.State.ExitCode}} oom_killed={{.State.OOMKilled}} error={{.State.Error}} started_at={{.State.StartedAt}}' \
			"$container" || true
		docker logs --tail "$log_lines" "$container" 2>&1 || true
	else
		it_log "$container does not exist"
	fi
	it_endgroup
done

# 準備完了の確認先へ問い合わせ、HTTP statusと応答の先頭を表示します。
probe() {
	local name=$1 url=$2
	shift 2
	local body
	body=$(curl -sS --max-time 5 -w '\n(HTTP %{http_code})' "$@" "$url" 2>&1 || true)
	it_log "$name $url -> ${body:0:2000}"
}

if ! command -v curl >/dev/null 2>&1; then
	exit 0
fi
it_group "readiness endpoints"
if it_loopback_port HETEROLOG_OPENSEARCH_ENDPOINT; then
	probe OpenSearch "http://127.0.0.1:$IT_PORT/_cluster/health"
fi
if it_loopback_port HETEROLOG_LOKI_ENDPOINT; then
	probe Loki "http://127.0.0.1:$IT_PORT/ready"
fi
if it_loopback_port HETEROLOG_CLICKHOUSE_ENDPOINT; then
	probe ClickHouse "http://127.0.0.1:$IT_PORT/ping"
fi
probe Tempo "http://127.0.0.1:$IT_TEMPO_HTTP_PORT/ready"
probe "OpenTelemetry Collector" "http://127.0.0.1:$IT_OTEL_COLLECTOR_HEALTH_PORT/"
it_endgroup
