#!/usr/bin/env bash
# start-services.shが起動したcontainer（匿名volumeを含む）とnetworkを削除します。
# 削除するのは結合テスト用のlabelを付けたものだけです。何も残っていない場合も成功します。
set -euo pipefail

# shellcheck source=testenv/integration/common.sh
. "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

it_require_commands docker
it_remove_services
it_log "removed the integration containers and network"
