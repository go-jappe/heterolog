#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || -z ${1:-} ]]; then
    printf 'Usage: %s NEW_OUTPUT_DIRECTORY\n' "$0" >&2
    exit 2
fi

# A new directory keeps retries from overwriting any existing test media.
if [[ -e $1 || -L $1 ]]; then
    printf 'Output directory already exists; refusing to overwrite: %s\n' "$1" >&2
    exit 2
fi
for required_command in go genisoimage realpath; do
    if ! command -v "$required_command" >/dev/null 2>&1; then
        printf 'Required command is unavailable: %s\n' "$required_command" >&2
        exit 2
    fi
done

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd -- "$script_dir/../.." && pwd -P)"
output_dir="$(realpath -m -- "$1")"
mkdir -- "$output_dir"
trap 'printf "Build failed; output retained at: %s\n" "$output_dir" >&2' ERR
payload_dir="$output_dir/payload"
mkdir -- "$payload_dir"

export GOOS=windows GOARCH=amd64 CGO_ENABLED=0
# go.modのgo versionに合わせたtoolchainを使います（cache済みならofflineで動きます）。
go_version=$(sed -n 's/^go \([0-9][0-9.]*\)$/\1/p' "$repo_root/go.mod")
if [[ -z $go_version ]]; then
    printf 'Cannot read the go version from go.mod\n' >&2
    exit 1
fi
export GOTOOLCHAIN="go$go_version" GOCACHE=/tmp/go-build-cache
cd -- "$repo_root"

# configの全体試験用。公開exampleから固定dummy値だけを生成し、実.envは読みません。
GOOS="$(go env GOHOSTOS)" GOARCH="$(go env GOHOSTARCH)" \
    go run -mod=readonly ./testenv/windows/configfixture .env.example "$payload_dir/config-fixture"

# package末尾が同じでも上書きしないよう、実行scriptが使うexe名を明示します。
# rootもheterolog libraryの通常試験として、全28 packageにtimetzdataを適用します。
package_binaries=(
    fsstore=internal/infrastructure/fsstore
    appendlog=internal/infrastructure/fsstore/appendlog
    failurearchive=internal/infrastructure/fsstore/failurearchive
    logfile=internal/infrastructure/logfile
    lifecycle=internal/infrastructure/logfile/lifecycle
    runner=internal/infrastructure/logfile/runner
    checkpoint=internal/infrastructure/fsstore/checkpoint
    recordspool=internal/infrastructure/fsstore/spool
    recordspoolstore=internal/infrastructure/fsstore/spool/store
    objectstorage=internal/infrastructure/fsstore/spool/objectstorage
    compressedlog=internal/infrastructure/fsstore/failurearchive/compressedlog
    pathidentity=pkg/path/pathidentity
    path=pkg/path
    directorylease=internal/infrastructure/fsstore/directorylease
    config=internal/config
    file=internal/infrastructure/logger/file
    fsarchivehandler=internal/infrastructure/logger/flushfailure/fsarchivehandler
    logpattern=internal/infrastructure/fsstore/name/logpattern
    compressedlogspool=internal/infrastructure/spool/compressedlog
    compressedlogfailure=internal/infrastructure/objectstorage/compressedlogfailure
    memorypressure=pkg/memorypressure
    pressure=internal/observer/memory/pressure
    heterolog=.
    compressedlog_worker=internal/infrastructure/spool/compressedlog/worker
    compressedlog_uploader=internal/infrastructure/spool/compressedlog/uploader
    compressedlog_s3_uploader=internal/infrastructure/spool/compressedlog/uploader/s3
    fsstore_config=pkg/fsstore
    mapped_config=internal/config/validation/mapped
)
for package_binary in "${package_binaries[@]}"; do
    name="${package_binary%%=*}"
    package="${package_binary#*=}"
    go test -mod=readonly -tags=timetzdata -c -o "$payload_dir/$name.test.exe" "./$package"
done

# Only the explicitly listed test binaries and runner enter the guest media.
# Do not copy the checkout, operational .env, or credentials. The only config
# files allowed here are the synthetic fixtures generated above.
cp -- "$script_dir/run-tests.cmd" "$payload_dir/run-tests.cmd"
cp -- "$script_dir/run-logger-tests.cmd" "$payload_dir/run-logger-tests.cmd"
cp -- "$script_dir/run-path-sync-tests.cmd" "$payload_dir/run-path-sync-tests.cmd"
cp -- "$script_dir/run-sharing-case-tests.cmd" "$payload_dir/run-sharing-case-tests.cmd"
cp -- "$script_dir/summarize-results.ps1" "$payload_dir/summarize-results.ps1"
cp -- "$script_dir/summarize-results_test.ps1" "$payload_dir/summarize-results_test.ps1"
iso_path="$output_dir/fsstore-windows-tests.iso"
genisoimage -quiet -J -R -V FSSTORE_TESTS -o "$iso_path" "$payload_dir"
chmod 0444 "$iso_path"
printf 'Payload: %s\nISO: %s\n' "$payload_dir" "$iso_path"
