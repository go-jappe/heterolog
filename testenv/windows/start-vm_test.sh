#!/usr/bin/env bash
set -euo pipefail

# 実QEMU/TPMや既存VMには触れず、このscript自身を一時mock executableとして使います。
if [[ ${1:-} == --mock-tpm ]]; then
    trap 'printf "tpm-stop\n" >> "$VM_TEST_CASE/events"; exit 0' TERM INT HUP
    printf 'tpm-start\n' >> "$VM_TEST_CASE/events"
    touch "$VM_TEST_CASE/tpm.ready"
    while :; do sleep 0.02; done
fi
case "${0##*/}" in
    swtpm)
        mock_pid_file=
        while (( $# > 0 )); do
            if [[ $1 == --pid ]]; then
                mock_pid_file=${2#file=}
                shift 2
            else
                shift
            fi
        done
        bash "$VM_TEST_SCRIPT" --mock-tpm &
        printf '%s\n' "$!" > "$mock_pid_file"
        while [[ ! -f "$VM_TEST_CASE/tpm.ready" ]]; do sleep 0.02; done
        if [[ $VM_TEST_MODE == startup ]]; then
            while [[ ! -f "$VM_TEST_CASE/startup.release" ]]; do sleep 0.02; done
        fi
        exit 0
        ;;
    qemu-kvm)
        printf '%s\n' "$@" > "$VM_TEST_CASE/arguments"
        printf '%s\n' "$$" > "$VM_TEST_CASE/qemu.mock.pid"
        trap 'printf "qemu-stop\n" >> "$VM_TEST_CASE/events"; exit 0' TERM INT HUP
        printf 'qemu-start\n' >> "$VM_TEST_CASE/events"
        touch "$VM_TEST_CASE/qemu.ready"
        if [[ $VM_TEST_MODE != wait ]]; then
            printf 'qemu-stop\n' >> "$VM_TEST_CASE/events"
            exit "$VM_TEST_MODE"
        fi
        while :; do sleep 0.02; done
        ;;
esac

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
test_dir=$(mktemp -d /tmp/windows-vm-lifecycle-test.XXXXXX)
export VM_TEST_SCRIPT="$script_dir/start-vm_test.sh"
mkdir "$test_dir/bin"
cp "$VM_TEST_SCRIPT" "$test_dir/bin/swtpm"
cp "$VM_TEST_SCRIPT" "$test_dir/bin/qemu-kvm"
chmod 0700 "$test_dir/bin/swtpm" "$test_dir/bin/qemu-kvm"
# 起動commandの絶対pathだけを検証コピーで置換し、製品にtest用hookは追加しません。
sed "s|/usr/libexec/qemu-kvm|$test_dir/bin/qemu-kvm|" \
    "$script_dir/start-vm.sh" > "$test_dir/start-vm.sh"
export PATH="$test_dir/bin:$PATH"

launcher_pid=
mock_qemu_pid=
mock_tpm_pid=
cleanup_test() {
    # 失敗時も、このtestが起動したprocessだけを終了します。診断fileは残します。
    if [[ -n "$launcher_pid" ]]; then
        kill -TERM "$launcher_pid" 2>/dev/null || true
        if [[ -n ${VM_TEST_CASE:-} ]]; then touch "$VM_TEST_CASE/startup.release"; fi
        wait "$launcher_pid" 2>/dev/null || true
    fi
    for mock_pid in "$mock_qemu_pid" "$mock_tpm_pid"; do
        if [[ -n "$mock_pid" ]]; then kill -TERM "$mock_pid" 2>/dev/null || true; fi
    done
    printf 'Mock lifecycle diagnostics: %s\n' "$test_dir"
}
trap cleanup_test EXIT

wait_for_file() {
    local path=$1
    for (( attempt=0; attempt<250; attempt++ )); do
        if [[ -f "$path" ]]; then return; fi
        sleep 0.02
    done
    printf 'Timed out waiting for %s\n' "$path" >&2
    return 1
}
wait_for_tpm_stop() {
    for (( attempt=0; attempt<250; attempt++ )); do
        if [[ $(<"$VM_TEST_CASE/events") == *tpm-stop ]]; then return; fi
        sleep 0.02
    done
    echo 'TPM did not stop' >&2
    return 1
}

for scenario in normal offline nat failure TERM INT HUP startup; do
    export VM_TEST_CASE="$test_dir/$scenario"
    export VM_TEST_MODE=wait
    expected_status=0
    case "$scenario" in
        normal|offline|nat) VM_TEST_MODE=0 ;;
        failure) VM_TEST_MODE=7; expected_status=7 ;;
        TERM|startup) expected_status=143 ;;
        INT) expected_status=130 ;;
        HUP) expected_status=129 ;;
    esac
    if [[ $scenario == startup ]]; then VM_TEST_MODE=startup; fi
    mkdir -p "$VM_TEST_CASE/state/tpm"
    printf 'mock system disk\n' > "$VM_TEST_CASE/state/system.qcow2"
    printf 'mock UEFI state\n' > "$VM_TEST_CASE/state/uefi-vars.fd"
    if [[ $scenario != normal ]]; then
        printf 'existing transfer image\n' > "$VM_TEST_CASE/state/results-transfer.img"
    fi
    : > "$VM_TEST_CASE/events"
    launcher_args=("$VM_TEST_CASE/state")
    if [[ $scenario == offline || $scenario == nat ]]; then
        launcher_args+=(- "" "$scenario")
    fi
    # job controlで非同期起動時のSIGINT無視を避け、対話起動と同じ条件にします。
    set -m
    bash "$test_dir/start-vm.sh" "${launcher_args[@]}" \
        > "$VM_TEST_CASE/launcher.log" 2>&1 &
    launcher_pid=$!
    set +m
    wait_for_file "$VM_TEST_CASE/tpm.ready"
    mock_tpm_pid=$(<"$VM_TEST_CASE/state/tpm.pid")
    if [[ $scenario == startup ]]; then
        kill -TERM "$launcher_pid"
        touch "$VM_TEST_CASE/startup.release"
    else
        wait_for_file "$VM_TEST_CASE/qemu.ready"
        mock_qemu_pid=$(<"$VM_TEST_CASE/qemu.mock.pid")
        if [[ $VM_TEST_MODE == wait ]]; then kill -"$scenario" "$launcher_pid"; fi
    fi
    actual_status=0
    wait "$launcher_pid" || actual_status=$?
    launcher_pid=
    if [[ $actual_status != "$expected_status" ]]; then
        printf '%s: exit=%s, want %s\n' "$scenario" "$actual_status" "$expected_status" >&2
        exit 1
    fi
    wait_for_tpm_stop
    if [[ $scenario == startup ]]; then
        expected_events=$'tpm-start\ntpm-stop'
    else
        expected_events=$'tpm-start\nqemu-start\nqemu-stop\ntpm-stop'
        if kill -0 "$mock_qemu_pid" 2>/dev/null; then
            echo 'QEMU child remained alive after launcher exit' >&2
            exit 1
        fi
        arguments=$(<"$VM_TEST_CASE/arguments")
        if [[ $scenario == nat ]]; then
            [[ $arguments == *$'-nic\nuser,model=e1000e\n'* ]]
            [[ $arguments != *$'-nic\nnone\n'* ]]
        else
            [[ $arguments == *$'-nic\nnone\n'* ]]
            [[ $arguments != *user,model=* ]]
        fi
        [[ $arguments != *hostfwd* ]]
        [[ $arguments != *guestfwd* ]]
        if [[ $scenario == normal ]]; then
            [[ $arguments != *results-transfer* ]]
        else
            [[ $arguments == *"file=$VM_TEST_CASE/state/results-transfer.img,format=raw,if=none,id=results-transfer,cache=writeback"* ]]
            [[ $arguments == *'usb-storage,drive=results-transfer,removable=on'* ]]
        fi
    fi
    [[ $(<"$VM_TEST_CASE/events") == "$expected_events" ]]
    [[ $(<"$VM_TEST_CASE/state/system.qcow2") == 'mock system disk' ]]
    [[ $(<"$VM_TEST_CASE/state/uefi-vars.fd") == 'mock UEFI state' ]]
    if [[ $scenario != normal ]]; then
        [[ $(<"$VM_TEST_CASE/state/results-transfer.img") == 'existing transfer image' ]]
    fi
    mock_qemu_pid=
    mock_tpm_pid=
    printf 'PASS %s\n' "$scenario"
done

# 未知のmodeはVM fileの検査より前に拒否し、TPMやserial logも作成しません。
export VM_TEST_CASE="$test_dir/invalid-network"
export VM_TEST_MODE=0
mkdir -p "$VM_TEST_CASE/state"
: > "$VM_TEST_CASE/events"
actual_status=0
bash "$test_dir/start-vm.sh" "$VM_TEST_CASE/state" - "" invalid \
    > "$VM_TEST_CASE/launcher.log" 2>&1 || actual_status=$?
[[ $actual_status == 2 ]]
[[ $(<"$VM_TEST_CASE/launcher.log") == *'Unknown network mode: invalid'* ]]
[[ ! -s "$VM_TEST_CASE/events" ]]
[[ ! -e "$VM_TEST_CASE/arguments" ]]
[[ -z $(ls -A "$VM_TEST_CASE/state") ]]
printf 'PASS invalid-network\n'
