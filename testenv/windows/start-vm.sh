#!/usr/bin/env bash
set -euo pipefail
umask 077

# 既に作成した専用VMを起動します。hostのdiskやdirectoryはguestへ共有しません。
if [[ $# -lt 1 || $# -gt 4 ]]; then
    echo "Usage: bash $0 STATE_DIR [WINDOWS_INSTALL_ISO|-] [TEST_MEDIA_ISO] [offline|nat]" >&2
    exit 2
fi
# 通常はofflineです。評価版activation/OOBE用のNATもport forwardingは行いません。
case "${4-offline}" in
    offline) network=(-nic none) ;;
    nat) network=(-nic user,model=e1000e) ;;
    *) echo "Unknown network mode: $4 (expected offline or nat)" >&2; exit 2 ;;
esac
vm_dir=$(realpath -- "$1")
install_iso=${2:--}
test_iso=${3:-}
for vm_file in system.qcow2 uefi-vars.fd; do
    if [[ ! -f "$vm_dir/$vm_file" ]]; then
        echo "Missing VM file: $vm_dir/$vm_file" >&2
        exit 1
    fi
done
if [[ -S "$vm_dir/qmp.sock" || -S "$vm_dir/tpm.sock" ]]; then
    echo "VM control socket already exists; check that the previous VM has stopped." >&2
    exit 1
fi

media=()
if [[ "$install_iso" != - ]]; then
    install_iso=$(realpath -- "$install_iso")
    if [[ ! -f "$install_iso" ]]; then
        echo "Missing Windows ISO: $install_iso" >&2
        exit 1
    fi
    media+=(-drive "file=$install_iso,media=cdrom,readonly=on,format=raw,if=ide,index=2")
fi
if [[ -n "$test_iso" ]]; then
    test_iso=$(realpath -- "$test_iso")
    if [[ ! -f "$test_iso" ]]; then
        echo "Missing test ISO: $test_iso" >&2
        exit 1
    fi
    media+=(-drive "file=$test_iso,media=cdrom,readonly=on,format=raw,if=ide,index=3")
fi

# 既存のログ持出し用imageだけを専用USBとして接続します。作成・初期化はしません。
transfer_media=()
if [[ -f "$vm_dir/results-transfer.img" ]]; then
    transfer_media+=(
        -drive "file=$vm_dir/results-transfer.img,format=raw,if=none,id=results-transfer,cache=writeback"
        -device usb-storage,drive=results-transfer,removable=on
    )
fi

qemu_pid=
tpm_pid=
shutdown_status=0
cleanup() {
    local status=$?
    trap - EXIT
    trap '' INT TERM HUP
    # launcherだけにsignalが届いた場合も、所有するQEMUを置き去りにしません。
    # QEMUがTPMを使わなくなるまで待ってからTPMを終了します。
    if [[ -n "$qemu_pid" ]]; then
        kill -TERM "$qemu_pid" 2>/dev/null || true
        wait "$qemu_pid" 2>/dev/null || true
    fi
    if [[ -n "$tpm_pid" ]]; then
        kill "$tpm_pid" 2>/dev/null || true
    fi
    return "$status"
}
request_shutdown() {
    shutdown_status=$1
    # 起動とPID取得の間に届いたsignalは、PID取得直後の判定まで保留します。
    if [[ -n "$qemu_pid" ]]; then
        exit "$shutdown_status"
    fi
}
trap cleanup EXIT
trap 'request_shutdown 130' INT
trap 'request_shutdown 143' TERM
trap 'request_shutdown 129' HUP

# TPMの状態もVM専用directoryへ保存します。QEMU切断後にTPM processも終了します。
swtpm socket --tpm2 --tpmstate "dir=$vm_dir/tpm" \
    --ctrl "type=unixio,path=$vm_dir/tpm.sock" --flags not-need-init \
    --terminate --daemon --pid "file=$vm_dir/tpm.pid"
tpm_pid=$(<"$vm_dir/tpm.pid")
if (( shutdown_status != 0 )); then
    exit "$shutdown_status"
fi

# KVMを使用できない場合は失敗させます。TCGへの暗黙の切り替えは行いません。
# VNC/QMPはnetwork modeにかかわらず所有者専用のUnix socketに限定します。
# cache=writebackはguestのflushを処理します。cache=unsafe/no-flushは使いません。
echo "Starting VM. VNC: $vm_dir/vnc.sock"
serial_log=$(mktemp "$vm_dir/serial-XXXXXX.log")
echo "Guest serial output: $serial_log"
/usr/libexec/qemu-kvm \
    -name heterolog-windows-tests \
    -machine q35,accel=kvm,smm=on -cpu host -smp 4 -m 8G \
    -global driver=cfi.pflash01,property=secure,value=on \
    -drive if=pflash,format=raw,readonly=on,file=/usr/share/edk2/ovmf/OVMF_CODE.secboot.fd \
    -drive "if=pflash,format=raw,file=$vm_dir/uefi-vars.fd" \
    -drive "file=$vm_dir/system.qcow2,format=qcow2,if=ide,index=0,cache=writeback" \
    "${media[@]}" \
    -chardev "socket,id=chrtpm,path=$vm_dir/tpm.sock" \
    -tpmdev emulator,id=tpm0,chardev=chrtpm -device tpm-tis,tpmdev=tpm0 \
    -device qemu-xhci -device usb-tablet -vga std \
    "${transfer_media[@]}" \
    "${network[@]}" -serial "file:$serial_log" -monitor none \
    -vnc "unix:$vm_dir/vnc.sock" \
    -qmp "unix:$vm_dir/qmp.sock,server=on,wait=off" \
    -pidfile "$vm_dir/qemu.pid" \
    -boot order=c,once=d,menu=on \
    -rtc base=localtime \
    -sandbox on,obsolete=deny,elevateprivileges=deny,spawn=deny,resourcecontrol=deny &
qemu_pid=$!
if (( shutdown_status != 0 )); then
    exit "$shutdown_status"
fi
qemu_status=0
wait "$qemu_pid" || qemu_status=$?
qemu_pid=
exit "$qemu_status"
