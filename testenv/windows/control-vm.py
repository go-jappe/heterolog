#!/usr/bin/env python3
"""既存の試験VMを、所有者専用Unix socket経由で操作する。認証情報は表示しない。"""

import argparse
import json
from pathlib import Path
import socket
import struct
import time


def receive_exact(connection, size):
    result = bytearray()
    while len(result) < size:
        part = connection.recv(size - len(result))
        if not part:
            raise RuntimeError("VM control connection closed")
        result.extend(part)
    return bytes(result)


def keyboard_leds(state_directory):
    """QEMUのLED拡張(-261)から状態だけを取得。画像・clipboardは要求しない。

    https://www.qemu.org/docs/master/interop/vnc-ledstate-pseudo-encoding.html
    """
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(5)
        connection.connect(str(state_directory / "vnc.sock"))
        if receive_exact(connection, 12) != b"RFB 003.008\n":
            raise RuntimeError("Expected local QEMU VNC protocol 3.8")
        connection.sendall(b"RFB 003.008\n")
        count = receive_exact(connection, 1)[0]
        if 1 not in receive_exact(connection, count):
            raise RuntimeError("Expected owner-only local VNC socket with no VNC authentication")
        connection.sendall(b"\x01")
        if struct.unpack(">I", receive_exact(connection, 4))[0] != 0:
            raise RuntimeError("Local VNC handshake failed")
        connection.sendall(b"\x01")  # 他のconsole接続を切断しないshared接続。
        header = receive_exact(connection, 24)
        receive_exact(connection, struct.unpack(">I", header[20:24])[0])
        connection.sendall(struct.pack(">BBHi", 2, 0, 1, -261))
        connection.sendall(struct.pack(">BBHHHH", 3, 0, 0, 0, 1, 1))
        while True:
            kind = receive_exact(connection, 1)[0]
            if kind == 2:  # Bell
                continue
            if kind != 0:
                raise RuntimeError("Unexpected VNC message while reading keyboard state")
            _, rectangles = struct.unpack(">BH", receive_exact(connection, 3))
            for _ in range(rectangles):
                _, _, width, height, encoding = struct.unpack(">HHHHi", receive_exact(connection, 12))
                if encoding == -261:
                    return receive_exact(connection, 1)[0]
                if encoding != 0:
                    raise RuntimeError("Unexpected VNC encoding while reading keyboard state")
                # 要求した1pixelの通常更新が先に来た場合だけ読み捨てる。
                receive_exact(connection, width * height * (header[4] // 8))


class VMControl:
    def __init__(self, state_directory):
        self.state_directory = state_directory
        self.connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.connection.settimeout(15)
        self.connection.connect(str(state_directory / "qmp.sock"))
        self.stream = self.connection.makefile("rwb", buffering=0)
        self.read_reply()
        self.call("qmp_capabilities")

    def close(self):
        self.stream.close()
        self.connection.close()

    def read_reply(self):
        while True:
            line = self.stream.readline()
            if not line:
                raise RuntimeError("QMP disconnected")
            reply = json.loads(line)
            if "event" not in reply:
                return reply

    def call(self, command, arguments=None):
        request = {"execute": command}
        if arguments is not None:
            request["arguments"] = arguments
        self.stream.write(json.dumps(request).encode() + b"\n")
        reply = self.read_reply()
        if "error" in reply:
            # 引数には入力文字が含まれ得るため、serverの詳細を出力しない。
            raise RuntimeError("QMP operation failed: " + command)
        return reply.get("return")

    def key_event(self, code, down):
        self.call("input-send-event", {"events": [{"type": "key", "data": {
            "down": down, "key": {"type": "qcode", "data": code},
        }}]})

    def key(self, codes):
        # 押下と解放を明示し、短すぎるsend-keyで文字が欠落しないよう間隔を置く。
        try:
            for code in codes:
                self.key_event(code, True)
            time.sleep(0.06)
        finally:
            for code in reversed(codes):
                self.key_event(code, False)
        time.sleep(0.06)

    def prepare_keyboard(self):
        # ログイン画面とログイン後で状態が変わるため、入力のたびに確認する。
        leds = keyboard_leds(self.state_directory)
        if leds & 4:
            self.key(["caps_lock"])
        if not leds & 2:
            self.key(["num_lock"])
        if keyboard_leds(self.state_directory) & 6 != 2:
            raise RuntimeError("Could not confirm CapsLock OFF and NumLock ON; input aborted")

    def click(self, x, y, width, height):
        if width < 2 or height < 2 or not (0 <= x < width and 0 <= y < height):
            raise ValueError("Click coordinates must be inside the supplied screen size")
        self.call("input-send-event", {"events": [
            {"type": "abs", "data": {"axis": "x", "value": x * 32767 // (width - 1)}},
            {"type": "abs", "data": {"axis": "y", "value": y * 32767 // (height - 1)}},
        ]})
        try:
            self.call("input-send-event", {"events": [
                {"type": "btn", "data": {"button": "left", "down": True}},
            ]})
            time.sleep(0.06)
        finally:
            self.call("input-send-event", {"events": [
                {"type": "btn", "data": {"button": "left", "down": False}},
            ]})

    def type_text(self, value):
        if any(not 32 <= ord(char) <= 126 for char in value):
            raise RuntimeError("Only printable ASCII input is supported")
        self.prepare_keyboard()
        for char in value:
            if "a" <= char <= "z" or "0" <= char <= "9":
                self.key([char])
            elif "A" <= char <= "Z":
                self.key(["shift", char.lower()])
            elif char == " ":
                self.key(["spc"])
            elif char == "-":
                self.key(["minus"])
            else:
                # 記号のキー配置がUS/JISで異なるため、NumLock確認後のAlt入力を使う。
                self.key_event("alt", True)
                try:
                    for digit in f"0{ord(char):03d}":
                        self.key(["kp_" + digit])
                finally:
                    self.key_event("alt", False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("state_directory", type=Path)
    parser.add_argument("action", choices=["status", "keyboard", "key", "click", "text", "password", "screen", "powerdown"])
    parser.add_argument("value", nargs="?")
    args = parser.parse_args()
    if args.action in ("key", "click", "text", "screen") and args.value is None:
        parser.error("this action requires a value")
    control = VMControl(args.state_directory)
    try:
        if args.action == "status":
            print(json.dumps(control.call("query-status")))
        elif args.action == "keyboard":
            leds = keyboard_leds(args.state_directory)
            print(json.dumps({"caps_lock": bool(leds & 4), "num_lock": bool(leds & 2)}))
        elif args.action == "key":
            control.key(args.value.split("-"))
        elif args.action == "click":
            control.click(*(int(part) for part in args.value.split(",")))
        elif args.action == "text":
            control.type_text(args.value)
        elif args.action == "password":
            # 認証情報を引数・stdout・screenへ出さない。Enterは別操作にする。
            secret = (args.state_directory / "login-password.txt").read_text(encoding="utf-8").rstrip("\r\n")
            control.type_text(secret)
            del secret
        elif args.action == "screen":
            destination = Path(args.value).absolute()
            if destination.exists():
                raise RuntimeError("Screenshot destination already exists")
            control.call("screendump", {"filename": str(destination)})
        elif args.action == "powerdown":
            control.call("system_powerdown")
            print("Sent guest ACPI power-button request; verify guest shutdown")
    finally:
        control.close()


if __name__ == "__main__":
    main()
