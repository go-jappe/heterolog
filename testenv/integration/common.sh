# shellcheck shell=bash
# 結合テストの共通定義です。start-services.sh・stop-services.sh・collect-diagnostics.sh・
# run-live-tests.shが読み込みます。単独では実行しません。

IT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
IT_REPO_ROOT=$(cd -- "$IT_DIR/../.." && pwd)

# image tagは版を固定します（latestは使いません）。更新する場合はREADME.mdの表も変更してください。
IT_OPENSEARCH_IMAGE=opensearchproject/opensearch:2.19.1
IT_LOKI_IMAGE=grafana/loki:3.4.2
IT_CLICKHOUSE_IMAGE=clickhouse/clickhouse-server:24.8.4.13
IT_TEMPO_IMAGE=grafana/tempo:2.7.2
IT_OTEL_COLLECTOR_IMAGE=otel/opentelemetry-collector-contrib:0.120.0
IT_VICTORIAMETRICS_IMAGE=victoriametrics/victoria-metrics:v1.106.1

# 起動するcontainerとnetworkにはこのlabelを付け、削除もこのlabelのものだけを対象にします。
IT_LABEL=heterolog.integration=ci
IT_NETWORK=heterolog-it
IT_OPENSEARCH_CONTAINER=heterolog-it-opensearch
IT_LOKI_CONTAINER=heterolog-it-loki
IT_CLICKHOUSE_CONTAINER=heterolog-it-clickhouse
IT_TEMPO_CONTAINER=heterolog-it-tempo
IT_OTEL_COLLECTOR_CONTAINER=heterolog-it-otel-collector
IT_VICTORIAMETRICS_CONTAINER=heterolog-it-victoriametrics
# shellcheck disable=SC2034 # collect-diagnostics.shが使います。
IT_CONTAINERS=(
	"$IT_OPENSEARCH_CONTAINER"
	"$IT_LOKI_CONTAINER"
	"$IT_CLICKHOUSE_CONTAINER"
	"$IT_TEMPO_CONTAINER"
	"$IT_OTEL_COLLECTOR_CONTAINER"
	"$IT_VICTORIAMETRICS_CONTAINER"
)

# 環境設定fileにはなく、準備完了の確認と診断だけに使うportです。TempoのHTTPのportは、
# 試験も使うため、環境設定fileのHETEROLOG_OTEL_TEMPO_QUERY_ADDRから読みます。
IT_OTEL_COLLECTOR_HEALTH_PORT=23133

# 宛先のportと認証情報を読む環境設定fileです。試験と同じくHETEROLOG_INTEGRATION_ENV_PATHを優先します。
# go testは各packageのdirectoryで試験を実行するため、相対pathは現在のdirectoryを基準に絶対pathへ直します。
IT_ENV_FILE=${HETEROLOG_INTEGRATION_ENV_PATH:-$IT_DIR/ci.env}
if [[ $IT_ENV_FILE != /* ]]; then
	IT_ENV_FILE=$PWD/$IT_ENV_FILE
fi

it_log() {
	printf '[integration] %s\n' "$*"
}

it_error() {
	printf '[integration] error: %s\n' "$*" >&2
}

# GitHub Actionsでは長い出力を折りたためるようにします。
it_group() {
	if [[ ${GITHUB_ACTIONS:-} == true ]]; then
		printf '::group::%s\n' "$*"
	else
		it_log "$*"
	fi
}

it_endgroup() {
	if [[ ${GITHUB_ACTIONS:-} == true ]]; then
		printf '::endgroup::\n'
	fi
}

it_require_commands() {
	local command
	for command in "$@"; do
		if ! command -v "$command" >/dev/null 2>&1; then
			it_error "$command is required"
			return 1
		fi
	done
}

# it_read_env KEY: 環境設定fileにあるKEYの値をIT_VALUEへ設定します。
# 後に書いた定義を使い、値全体を囲む引用符だけを外します（escapeや変数展開は扱いません）。
# 項目がない場合や値が空の場合は失敗します。
it_read_env() {
	local key=$1 line= value= found=
	IT_VALUE=
	if [[ ! $key =~ ^[A-Z][A-Z0-9_]*$ ]]; then
		it_error "invalid environment key: $key"
		return 1
	fi
	if [[ ! -r $IT_ENV_FILE ]]; then
		it_error "environment file is not readable: $IT_ENV_FILE"
		return 1
	fi
	while IFS= read -r line || [[ -n $line ]]; do
		line=${line%$'\r'}
		line=${line#export }
		if [[ $line == "$key="* ]]; then
			value=${line#"$key="}
			found=1
		fi
	done <"$IT_ENV_FILE"
	if [[ ${#value} -ge 2 && ${value:0:1} == "${value: -1}" && ( ${value:0:1} == '"' || ${value:0:1} == "'" ) ]]; then
		value=${value:1:${#value}-2}
	else
		# 引用符のない値では、空白に続く#以降をcommentとして外します。
		value=${value%%[[:space:]]#*}
		value=${value%"${value##*[![:space:]]}"}
	fi
	if [[ -z $found || -z $value ]]; then
		it_error "$key is not set in $IT_ENV_FILE"
		return 1
	fi
	IT_VALUE=$value
}

# it_loopback_port KEY: KEYの宛先（http://127.0.0.1:PORT[/path] または 127.0.0.1:PORT）の
# portをIT_PORTへ設定します。containerは127.0.0.1だけで公開するため、他の宛先は拒否します。
it_loopback_port() {
	local key=$1 hostport
	IT_PORT=
	it_read_env "$key" || return 1
	hostport=${IT_VALUE#*://}
	hostport=${hostport%%/*}
	if [[ ! $hostport =~ ^(127\.0\.0\.1|localhost):([0-9]+)$ ]]; then
		it_error "$key=$IT_VALUE must point to 127.0.0.1:<port> because the containers are published only on the loopback address"
		return 1
	fi
	IT_PORT=${BASH_REMATCH[2]}
}

# 127.0.0.1:PORTで既に待ち受けているprocessがあれば失敗します。
it_require_free_port() {
	local port=$1
	if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
		it_error "127.0.0.1:$port is already in use; stop that service or change the port in $IT_ENV_FILE"
		return 1
	fi
}

# imageが手元になければ取得します。一時的な通信失敗に備えて3回まで試します。
# tagが存在しない場合と取得回数の上限に達した場合は、再試行しても解消しないため直ちに失敗します。
it_pull_image() {
	local image=$1 attempt output
	if docker image inspect "$image" >/dev/null 2>&1; then
		it_log "image $image is already present"
		return 0
	fi
	for attempt in 1 2 3; do
		if output=$(docker pull --quiet "$image" 2>&1); then
			it_log "pulled $output"
			return 0
		fi
		it_error "docker pull $image failed (attempt $attempt/3): $output"
		case $output in
		*"manifest unknown"* | *"not found"* | *toomanyrequests* | *"pull access denied"*)
			break
			;;
		esac
		if ((attempt < 3)); then
			sleep $((attempt * 10))
		fi
	done
	it_error "could not pull $image; check the image tag and the network connection"
	return 1
}

# start-services.shが作成したcontainer（匿名volumeを含む）とnetworkを削除します。
it_remove_services() {
	local ids
	ids=$(docker ps -aq --filter "label=$IT_LABEL") || return 1
	if [[ -n $ids ]]; then
		# shellcheck disable=SC2086 # container IDを個別の引数として渡します。
		docker rm -f -v $ids >/dev/null || return 1
	fi
	ids=$(docker network ls -q --filter "label=$IT_LABEL") || return 1
	if [[ -n $ids ]]; then
		# shellcheck disable=SC2086 # network IDを個別の引数として渡します。
		docker network rm $ids >/dev/null || return 1
	fi
}

# it_wait_ready NAME CONTAINER URL TIMEOUT_SECONDS [curlの追加引数...]
# URLが成功応答を返すまで2秒間隔で確認します。containerが停止した場合と、
# TIMEOUT_SECONDSを過ぎた場合は失敗します。
it_wait_ready() {
	local name=$1 container=$2 url=$3 timeout=$4
	shift 4
	local started=$SECONDS attempts=0 last_report=$SECONDS running
	it_log "waiting for $name at $url (up to ${timeout}s)"
	while :; do
		attempts=$((attempts + 1))
		if curl -fsS --max-time 10 -o /dev/null "$@" "$url" 2>/dev/null; then
			it_log "$name is ready ($((SECONDS - started))s, $attempts attempts)"
			return 0
		fi
		running=$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || true)
		if [[ $running != true ]]; then
			it_error "$name container $container is not running"
			return 1
		fi
		if ((SECONDS - started >= timeout)); then
			it_error "$name was not ready within ${timeout}s ($attempts attempts)"
			return 1
		fi
		if ((SECONDS - last_report >= 15)); then
			it_log "still waiting for $name ($((SECONDS - started))s)"
			last_report=$SECONDS
		fi
		sleep 2
	done
}
