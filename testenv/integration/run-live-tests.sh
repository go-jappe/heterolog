#!/usr/bin/env bash
# opt-inの結合テスト（実serviceへ接続する試験）を、試験対象のgroupごとに実行します。
#
#   testenv/integration/run-live-tests.sh [GROUP...]
#
# GROUPはopensearch・opensearch-client・loki・clickhouse・tracingです。省略すると全groupを実行します。
# 各groupの試験だけを-runで選び、opt-inの変数とHETEROLOG_INTEGRATION_ENV_PATH（未指定なら
# 同じdirectoryのci.env）を設定してgo test -count=1で実行します。選んだ試験にPASSしなかったもの
# （失敗のほか、skipや試験名の変更で実行されなかったもの）があれば失敗します。
set -euo pipefail

# shellcheck source=testenv/integration/common.sh
. "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

it_require_commands go
if [[ ! -r $IT_ENV_FILE ]]; then
	it_error "environment file is not readable: $IT_ENV_FILE"
	exit 1
fi
# 試験は専用fileだけから設定を読みます。go testは各packageのdirectoryで試験を実行するため、絶対pathで渡します。
export HETEROLOG_INTEGRATION_ENV_PATH=$IT_ENV_FILE

go_test_timeout=300s
all_groups=(opensearch opensearch-client loki clickhouse tracing)

# select_group GROUP: groupのopt-in変数、package、試験名を設定します。
# どの試験もHETEROLOG_INTEGRATION_ENV_PATHの専用fileだけから設定を読みます。
select_group() {
	opt_in=()
	packages=()
	tests=()
	case $1 in
	opensearch)
		opt_in=(OPENSEARCH_INTEGRATION_TEST)
		packages=(./internal/infrastructure/logger)
		tests=(
			TestOpenSearchLoggerUsesActualDotEnvConfigAndStoresDocument
			TestOpenSearchSpoolUsesActualDotEnvConfigActiveAndResendsToBackend
		)
		;;
	opensearch-client)
		opt_in=(OPENSEARCH_INTEGRATION_TEST)
		packages=(./pkg/opensearch)
		tests=(
			TestIndexDocumentToLocalOpenSearch
			TestIndexDocumentsToLocalOpenSearch
			TestIndexDocumentCompressedToLocalOpenSearch
			TestIndexDocumentsCompressedToLocalOpenSearch
		)
		;;
	loki)
		opt_in=(LOKI_INTEGRATION_TEST)
		packages=(./internal/infrastructure/logger ./internal/infrastructure/logger/loki)
		tests=(
			TestLokiLoggerUsesActualDotEnvConfigAndStoresEntry
			TestLokiSpoolUsesActualDotEnvConfigActiveAndResendsToBackend
			TestLokiLoggerWithIntegrationEnvironmentPushesToLocalLoki
		)
		;;
	clickhouse)
		opt_in=(CLICKHOUSE_INTEGRATION_TEST)
		packages=(./internal/infrastructure/logger)
		tests=(
			TestClickHouseLoggerUsesActualDotEnvConfigAndStoresRow
			TestClickHouseSpoolUsesActualDotEnvConfigActiveAndResendsToBackend
		)
		;;
	tracing)
		opt_in=(TEMPO_INTEGRATION_TEST OTEL_COLLECTOR_INTEGRATION_TEST)
		packages=(./internal/infrastructure/logger/file ./internal/bootstrap)
		tests=(
			TestFileLogger_Log_WritesToFile_ForTempo
			TestFileLogger_Log_WritesToFile_ForTempo_Via_Collector
			TestTelemetrySendsSpansToTempoOverOTLPHTTP
			TestTelemetryResendsSpooledSpansToTempo
		)
		;;
	*)
		it_error "unknown group: $1 (groups: ${all_groups[*]})"
		return 1
		;;
	esac
}

# run_group GROUP: groupの試験を実行し、選んだ試験がすべてPASSしたかを確認します。
# 呼び出し側の条件式の中でも動くよう、失敗はすべて明示的に返します。
run_group() {
	local group=$1 regex output status=0 name
	local -a env_args=()
	select_group "$group" || return 1
	regex="^($(
		IFS='|'
		printf '%s' "${tests[*]}"
	))\$"
	for name in "${opt_in[@]}"; do
		env_args+=("$name=true")
	done
	output=$(mktemp) || return 1
	it_log "live tests: $group (HETEROLOG_INTEGRATION_ENV_PATH=$HETEROLOG_INTEGRATION_ENV_PATH)"
	it_log "${env_args[*]} go test -count=1 -timeout $go_test_timeout -v -run '$regex' ${packages[*]}"
	(cd "$IT_REPO_ROOT" && env "${env_args[@]}" go test -count=1 -timeout "$go_test_timeout" -v -run "$regex" "${packages[@]}") 2>&1 |
		tee "$output" || status=$?
	for name in "${tests[@]}"; do
		if ! grep -Eq "^--- PASS: $name \(" "$output"; then
			it_error "$group: $name did not pass (failed, skipped, or not found)"
			status=1
		fi
	done
	rm -f "$output"
	if ((status == 0)); then
		it_log "$group: all ${#tests[@]} live tests passed"
	fi
	return "$status"
}

groups=("$@")
if ((${#groups[@]} == 0)); then
	groups=("${all_groups[@]}")
fi
# 名前の誤りは、試験を始める前に検出します。
for group in "${groups[@]}"; do
	select_group "$group"
done

failed=()
for group in "${groups[@]}"; do
	run_group "$group" || failed+=("$group")
done
if ((${#failed[@]} > 0)); then
	it_error "live test groups failed: ${failed[*]}"
	exit 1
fi
it_log "live test groups passed: ${groups[*]}"
