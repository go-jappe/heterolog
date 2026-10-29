#!/usr/bin/env bash
# macOSの実機で、CIのmacOS runnerでは用意できない条件の試験を実行します。
#
#   bash testenv/macos/run-real-machine-tests.sh [RESULTS_DIRECTORY]
#
#  1. 既定の一時directory（通常は大小文字を区別しないAPFS）で全packageと、CIのLinuxと同じ
#     race試験の一覧を実行します。
#  2. 大小文字を区別するAPFSのdisk imageを試験の一時directoryにして、全packageを実行します。
#  3. ExFATとFAT32のdisk imageを試験の一時directoryにして、大小文字の判定の試験と、大小文字を
#     区別しない一時directoryでだけ動くfile loggerの試験を実行し、skipや未実行も失敗にします。
#     同じ条件でpathidentityとfile loggerのpackage全体も参考として実行します。macOSがFAT32・ExFATに
#     拡張属性を保存する._のファイルなどで失敗するため、参考の段階の失敗は全体の結果に含めません。
#  4. Dockerが使えれば、testenv/integrationの結合試験を全group実行します。
#
# disk imageはhdiutilでsparse imageとして作成し、root権限なしでmountします。終了時（失敗・
# 中断を含む）にdetachして削除し、結合試験のcontainerも停止します。結果はRESULTS_DIRECTORY
# （省略時は$HOME/heterolog-macos-results-日時）のsummary.txtと各段階のlogに残します。
# macOS標準のbash 3.2で動くように書いています。
set -u

if [ "$(uname -s)" != Darwin ]; then
	echo "this script runs only on macOS" >&2
	exit 2
fi
for command in go git hdiutil; do
	if ! command -v "$command" >/dev/null 2>&1; then
		echo "required command not found: $command" >&2
		exit 2
	fi
done

repo_root=$(cd "$(dirname "$0")/../.." && pwd)
results=${1:-"$HOME/heterolog-macos-results-$(date +%Y%m%d-%H%M%S)"}
if [ -e "$results" ]; then
	echo "results directory already exists: $results" >&2
	exit 2
fi
mkdir -p "$results" || exit 2
results=$(cd "$results" && pwd)
summary=$results/summary.txt
# macOSのTMPDIRは末尾が/のため、そのまま連結すると試験の一時directoryに「//」が入ります。
# 正規化したpathの下に作り、各段階が意図した条件だけで動くようにします（linkは解決しません）。
temporary_root=$(cd "${TMPDIR:-/tmp}" && pwd) || exit 2
work=$(mktemp -d "$temporary_root/heterolog-macos.XXXXXX") || exit 2
mountpoints=()
failures=0
integration_started=false
# 結合試験は使い捨てのlocal container向けの専用設定だけを使い、開発者の設定の宛先へは送りません。
integration_env=$repo_root/testenv/integration/ci.env

cleanup() {
	local mountpoint
	if [ "$integration_started" = true ]; then
		HETEROLOG_INTEGRATION_ENV_PATH=$integration_env \
			bash "$repo_root/testenv/integration/stop-services.sh" >>"$results/integration-stop.log" 2>&1
	fi
	for mountpoint in ${mountpoints[@]+"${mountpoints[@]}"}; do
		hdiutil detach -quiet "$mountpoint" >/dev/null 2>&1 ||
			hdiutil detach -force -quiet "$mountpoint" >/dev/null 2>&1
	done
	rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# record NAME RESULT [DETAIL]: 段階の結果をsummary.txtと標準出力へ書きます。
record() {
	printf '%-36s %-5s %s\n' "$1" "$2" "${3:-}" | tee -a "$summary"
}

# go_test NAME TMPDIR ARGS...: go test -count=1の出力をNAME.logへ残します。TMPDIRが空でなければ、
# t.TempDirが優先するGOTMPDIRとTMPDIRの両方を試験processへだけ渡し、go自身の作業は既定の
# 一時directoryに置きます。
go_test() {
	local name=$1 tmpdir=$2
	shift 2
	if [ -n "$tmpdir" ]; then
		# go testは-execの値を引用符付きで分割するため、空白を含むpathも渡せます。
		(cd "$repo_root" && go test -count=1 -exec "env 'GOTMPDIR=$tmpdir' 'TMPDIR=$tmpdir'" "$@") >"$results/$name.log" 2>&1
	else
		(cd "$repo_root" && go test -count=1 "$@") >"$results/$name.log" 2>&1
	fi
}

# suite NAME REQUIRED TMPDIR ARGS...: go testを実行して結果を記録します。REQUIREDがfalseの段階は
# 参考として記録し、失敗しても全体の結果に含めません。
suite() {
	local name=$1 required=$2 tmpdir=$3
	shift 3
	if go_test "$name" "$tmpdir" "$@"; then
		record "$name" PASS
	elif [ "$required" = true ]; then
		record "$name" FAIL "$(grep -E '^(--- FAIL|FAIL|panic)' "$results/$name.log" | head -n 5 | tr '\n' ' ')"
		failures=$((failures + 1))
	else
		record "$name" FAIL "(reference only) see $name.log"
	fi
}

# case_tests NAME TMPDIR PACKAGE TEST...: 指定した試験だけを実行し、すべてがPASSしたことを
# 確かめます。失敗のほか、skipや試験名の変更で実行されなかったものも失敗にします。
case_tests() {
	local name=$1 tmpdir=$2 package=$3 regex test missing=""
	shift 3
	regex="^($(
		IFS='|'
		printf '%s' "$*"
	))\$"
	go_test "$name" "$tmpdir" -v -run "$regex" "$package"
	for test in "$@"; do
		if ! grep -Eq "^--- PASS: $test \(" "$results/$name.log"; then
			missing="$missing $test"
		fi
	done
	if [ -z "$missing" ]; then
		record "$name" PASS
	else
		record "$name" FAIL "did not pass (failed, skipped or not run):$missing"
		failures=$((failures + 1))
	fi
}

# make_volume NAME FILESYSTEM VOLUME_NAME SIZE EXPECTED: sparse disk imageを作成してmountし、
# 試験の一時directoryにするpathをvolume_tmpへ設定します。EXPECTEDはsensitiveかinsensitiveで、
# mountしたvolumeが実際にそう扱うことを、作成したfileを別表記で開けるかで確かめます。
make_volume() {
	local name=$1 filesystem=$2 volume=$3 size=$4 expected=$5
	local image=$work/$name mountpoint=$work/mnt-$name actual
	volume_tmp=""
	mkdir "$mountpoint" || return 1
	echo "== $name: hdiutil create -fs '$filesystem' -size $size" >>"$results/volumes.log"
	hdiutil create -type SPARSE -size "$size" -fs "$filesystem" -volname "$volume" "$image" >>"$results/volumes.log" 2>&1 ||
		return 1
	hdiutil attach -nobrowse -noautoopen -mountpoint "$mountpoint" "$image.sparseimage" >>"$results/volumes.log" 2>&1 ||
		return 1
	mountpoints+=("$mountpoint")
	mount | grep "mnt-$name" >>"$results/volumes.log"
	mkdir "$mountpoint/tmp" || return 1
	touch "$mountpoint/tmp/Probe.log" || return 1
	if [ -e "$mountpoint/tmp/PROBE.LOG" ]; then
		actual=insensitive
	else
		actual=sensitive
	fi
	rm -f "$mountpoint/tmp/Probe.log"
	if [ "$actual" != "$expected" ]; then
		echo "$name: expected a case-$expected volume, but it is case-$actual" >>"$results/volumes.log"
		return 1
	fi
	volume_tmp=$mountpoint/tmp
}

{
	echo "date: $(date)"
	echo "macOS: $(sw_vers -productVersion) ($(sw_vers -buildVersion)) $(uname -m)"
	echo "commit: $(git -C "$repo_root" rev-parse HEAD)"
	echo "changed files: $(git -C "$repo_root" status --porcelain | wc -l | tr -d ' ')"
	(cd "$repo_root" && go version)
	echo "default temporary directory: ${TMPDIR:-/tmp}"
	df -h "${TMPDIR:-/tmp}" | tail -n 1
	if command -v docker >/dev/null 2>&1; then
		docker version --format 'docker: client {{.Client.Version}}, server {{.Server.Version}}' 2>&1
	fi
} >"$results/environment.txt" 2>&1

file_case_tests="TestCompressedArchiveGroupsPathIdentityPreservesUploadPath TestFileLoggerCaseInsensitiveTargetCaseRestartPreservesGenerationsAndRetriesArchive TestLoggerSetPathIdentityRejectsCaseAliasedFilePrefixes"
race_packages=". ./internal/application/logging ./internal/application/trace/tracer ./internal/infrastructure/logger ./internal/infrastructure/logger/multi ./internal/infrastructure/logger/slack ./internal/infrastructure/logger/opensearch ./internal/infrastructure/fsstore/spool/... ./internal/infrastructure/spool/log/..."

# 1. 既定の一時directory。
suite default-disk-all-packages true "" ./...
# shellcheck disable=SC2086 # package一覧は空白で区切って渡します。
suite default-disk-race true "" -race $race_packages
case_tests default-disk-detection "" ./pkg/path/pathidentity TestDarwinDirectoryCaseMatchesTemporaryFilesystem

# 2. 大小文字を区別するAPFS。設定例のHETEROLOG_MIN_AVAILABLE_DISK_SPACE_GB（1GB）を満たす容量にします。
if make_volume case-sensitive-apfs "Case-sensitive APFS" HLCaseSensitive 16g sensitive; then
	suite case-sensitive-apfs-all-packages true "$volume_tmp" ./...
	case_tests case-sensitive-apfs-detection "$volume_tmp" ./pkg/path/pathidentity TestDarwinDirectoryCaseMatchesTemporaryFilesystem
else
	record case-sensitive-apfs FAIL "could not create or mount the volume; see volumes.log"
	failures=$((failures + 1))
fi

# 3. ExFATとFAT32。FATのvolume名は11文字以内の大文字です。
for spec in "exfat ExFAT HLEXFAT" "fat32 MS-DOS_FAT32 HLFAT32"; do
	set -- $spec
	name=$1 filesystem=$(printf '%s' "$2" | tr _ ' ') volume=$3
	if make_volume "$name" "$filesystem" "$volume" 1g insensitive; then
		case_tests "$name-detection" "$volume_tmp" ./pkg/path/pathidentity TestDarwinDirectoryCaseMatchesTemporaryFilesystem
		# shellcheck disable=SC2086 # 試験名は空白で区切って渡します。
		case_tests "$name-file-logger-case" "$volume_tmp" ./internal/infrastructure/logger/file $file_case_tests
		suite "$name-pathidentity-reference" false "$volume_tmp" ./pkg/path/pathidentity
		suite "$name-file-logger-reference" false "$volume_tmp" ./internal/infrastructure/logger/file
	else
		record "$name" FAIL "could not create or mount the volume; see volumes.log"
		failures=$((failures + 1))
	fi
done

# 4. Docker上の結合試験。
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
	integration_started=true
	if HETEROLOG_INTEGRATION_ENV_PATH=$integration_env \
		bash "$repo_root/testenv/integration/start-services.sh" >"$results/integration-start.log" 2>&1 &&
		HETEROLOG_INTEGRATION_ENV_PATH=$integration_env \
			bash "$repo_root/testenv/integration/run-live-tests.sh" >"$results/integration-live-tests.log" 2>&1; then
		record integration PASS
	else
		record integration FAIL "see integration-start.log and integration-live-tests.log"
		failures=$((failures + 1))
	fi
	HETEROLOG_INTEGRATION_ENV_PATH=$integration_env \
		bash "$repo_root/testenv/integration/stop-services.sh" >"$results/integration-stop.log" 2>&1
	integration_started=false
else
	record integration SKIP "docker is not available"
fi

echo "results: $results"
if [ "$failures" -gt 0 ]; then
	echo "$failures required step(s) failed" | tee -a "$summary"
	exit 1
fi
echo "all required steps passed" | tee -a "$summary"
