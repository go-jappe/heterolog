#!/usr/bin/env bash
set -euo pipefail

# CIの`go test ./...`と同じく、testを持つ全packageをWindows nativeで実行する媒体を作ります。
# 媒体にはgit checkoutに含まれる範囲のsource tree（source.tar）、package別のtest exe、
# guest用の実行・集計scriptだけを入れます。実運用.envや認証情報は持ち込みません。
if [[ $# -ne 1 || -z ${1:-} ]]; then
    printf 'Usage: %s NEW_OUTPUT_DIRECTORY\n' "$0" >&2
    exit 2
fi

# A new directory keeps retries from overwriting any existing test media.
if [[ -e $1 || -L $1 ]]; then
    printf 'Output directory already exists; refusing to overwrite: %s\n' "$1" >&2
    exit 2
fi
for required_command in go genisoimage isoinfo realpath rsync tar sha256sum; do
    if ! command -v "$required_command" >/dev/null 2>&1; then
        printf 'Required command is unavailable: %s\n' "$required_command" >&2
        exit 2
    fi
done

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd -- "$script_dir/../.." && pwd -P)"
output_dir="$(realpath -m -- "$1")"
# checkoutの複製へ出力自身が入らないよう、作業tree内の出力先は拒否します。
case "$output_dir/" in
    "$repo_root/"*)
        printf 'Output directory must be outside the repository: %s\n' "$output_dir" >&2
        exit 2
        ;;
esac
mkdir -- "$output_dir"
trap 'printf "Build failed; output retained at: %s\n" "$output_dir" >&2' ERR
source_dir="$output_dir/source"
payload_dir="$output_dir/payload"
mkdir -- "$payload_dir" "$payload_dir/bin"

# git checkoutに含まれる範囲だけを複製します。.gitignoreの除外対象（/.env、/.env.*、
# 各種tool directory、build/test出力）、作業用の/.claude/、remote設定を含み得る/.gitは持ち込みません。
# rsyncは最初に一致したruleを使うため、公開exampleの許可を先に置きます。
rsync -a \
    --include=/.env.example --exclude=/.env --exclude='/.env.*' \
    --exclude=/.aws/ --exclude=/.agents/ --exclude=/.codex/ --exclude=/.vscode/ \
    --exclude=/.claude/ --exclude=/.git --exclude=.DS_Store \
    --exclude='*.test' --exclude='*.test.exe' --exclude='*.prof' \
    --exclude=coverage.out --exclude=coverage.html --exclude=fixture-storage/ \
    --exclude=/main --exclude='/log-recovery*' --exclude=/bin/ --exclude=/dist/ \
    --exclude=/error_logs/ --exclude=/logs/ \
    -- "$repo_root/" "$source_dir/"

# 除外漏れがあれば媒体を作らずに止めます。入れ子の.envも公開example以外は拒否します。
forbidden_entries=$(cd -- "$source_dir" && find . \
    \( -name .env -o \( -name '.env.*' ! -name .env.example \) \
    -o -name .aws -o -name .agents -o -name .codex -o -name .vscode -o -name .claude -o -name .git \
    -o -name '*.test' -o -name '*.test.exe' -o -name fixture-storage \
    -o -name 'login-password.txt' -o -name 'recovery-answer-*.txt' \) -print)
if [[ -n $forbidden_entries ]]; then
    printf 'Refusing to package excluded or credential files:\n%s\n' "$forbidden_entries" >&2
    exit 1
fi
if [[ ! -f $source_dir/.env.example || ! -f $source_dir/go.mod ]]; then
    printf 'Source snapshot is missing go.mod or .env.example\n' >&2
    exit 1
fi

# guestではWindows同梱のtar.exeで展開します。名前順・所有者固定で再現しやすくします。
tar --sort=name --owner=0 --group=0 --numeric-owner --format=pax \
    -cf "$payload_dir/source.tar" -C "$source_dir" .
tar -tf "$payload_dir/source.tar" | LC_ALL=C sort > "$output_dir/source-files.txt"
# guestでは試験後のcheckout複製と比べ、試験が追加・削除したfileを報告します。
cp -- "$output_dir/source-files.txt" "$payload_dir/source-files.txt"

# go.modのgo versionに合わせたtoolchainを使います（cache済みならofflineで動きます）。
go_version=$(sed -n 's/^go \([0-9][0-9.]*\)$/\1/p' "$source_dir/go.mod")
if [[ -z $go_version ]]; then
    printf 'Cannot read the go version from go.mod\n' >&2
    exit 1
fi
export GOTOOLCHAIN="go$go_version"
# 複製にはVCS情報がなく、上位directoryのrepositoryも参照しないようにVCS stampを無効にします。
export GOOS=windows GOARCH=amd64 CGO_ENABLED=0 GOFLAGS='-mod=readonly -buildvcs=false'
# 複製したsnapshotからbuildし、媒体のsourceとexeの内容を一致させます。
cd -- "$source_dir"
go version > "$output_dir/go-version.txt"
module_path=$(go list -m)

# CIと同じくtest fileを持つ全packageを対象にします。package末尾の名前は重複するため、
# 媒体上のexe名はmodule rootからのdirectoryを`-`でつないだ一意な名前にします。
# guestでは`go test`と同じくpackage末尾名のexeとして一時directoryから起動します。
go list -tags=timetzdata -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./... \
    > "$output_dir/test-packages.txt"
manifest="$payload_dir/packages.tsv"
printf 'name\timport_path\tdirectory\tbinary\n' > "$manifest"
package_count=0
while IFS= read -r import_path; do
    relative=${import_path#"$module_path"}
    relative=${relative#/}
    if [[ -z $relative ]]; then
        directory=.
        name=${module_path##*/}
    else
        directory=$relative
        name=${relative//\//-}
    fi
    binary=${import_path##*/}
    if [[ -e $payload_dir/bin/$name.test.exe ]]; then
        printf 'Duplicate test executable name: %s\n' "$name" >&2
        exit 1
    fi
    go test -tags=timetzdata -c -o "$payload_dir/bin/$name.test.exe" "./$directory"
    printf '%s\t%s\t%s\t%s\n' "$name" "$import_path" "$directory" "$binary" >> "$manifest"
    package_count=$((package_count + 1))
done < "$output_dir/test-packages.txt"
if (( package_count == 0 )); then
    printf 'No test packages were found\n' >&2
    exit 1
fi

# CLIはtest fileを持たないため、起動・usage表示だけを確認するexeを別に入れます。
go build -tags=timetzdata -o "$payload_dir/bin/log-recovery.exe" ./cmd/log-recovery

cp -- "$script_dir/run-all-package-tests.cmd" "$payload_dir/run-all-package-tests.cmd"
cp -- "$script_dir/run-all-package-tests.ps1" "$payload_dir/run-all-package-tests.ps1"
cp -- "$script_dir/summarize-all-package-results.ps1" "$payload_dir/summarize-all-package-results.ps1"
cp -- "$script_dir/summarize-all-package-results_test.ps1" "$payload_dir/summarize-all-package-results_test.ps1"
# 既存の集計scriptも同じ`logs` directoryを読めるため、照合と自己試験用に同梱します。
cp -- "$script_dir/summarize-results.ps1" "$payload_dir/summarize-results.ps1"
cp -- "$script_dir/summarize-results_test.ps1" "$payload_dir/summarize-results_test.ps1"

# 媒体へ入れる全fileとsource.tar内の全entryを一覧にし、除外対象がないことを再確認します。
(cd -- "$payload_dir" && find . -type f | LC_ALL=C sort) > "$output_dir/payload-files.txt"
if grep -E '(^|/)(\.env(\..*)?|\.aws|\.agents|\.codex|\.vscode|\.claude|\.git|login-password\.txt|recovery-answer-[^/]*\.txt)(/|$)' \
    "$output_dir/payload-files.txt" "$output_dir/source-files.txt" | grep -vE '(^|/)\.env\.example$'; then
    printf 'Refusing to build media with excluded or credential files\n' >&2
    exit 1
fi

iso_path="$output_dir/heterolog-windows-all-tests.iso"
# exe名は64文字を超えるため、Jolietの長い名前を有効にします。
genisoimage -quiet -J -joliet-long -R -V HETEROLOG_TESTS -o "$iso_path" "$payload_dir"
chmod 0444 "$iso_path"
isoinfo -J -f -i "$iso_path" | LC_ALL=C sort > "$output_dir/iso-files.txt"
(cd -- "$payload_dir" && sha256sum bin/*.exe source.tar packages.tsv ./*.cmd ./*.ps1) > "$output_dir/payload-sha256.txt"
sha256sum "$iso_path" > "$output_dir/iso-sha256.txt"
printf 'Packages: %s\nPayload: %s\nISO: %s\n' "$package_count" "$payload_dir" "$iso_path"
