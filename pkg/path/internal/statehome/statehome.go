// Package statehomeは、Linuxなどのunix系OSでrootで動いていない利用者について、保存先の
// 既定の基点directoryにするstate directory（XDG Base Directory仕様）を決めます。
package statehome

import (
	"os"
	"path/filepath"
)

// UserDirは、rootで動いていない場合に、利用者のstate directoryとtrueを返します。
// $XDG_STATE_HOMEが絶対pathならそれを、そうでなければ$HOME/.local/stateを使います。
// rootで動く場合と、home directoryが分からない場合はfalseを返します。呼び出し側は
// /var/logなどのsystem directoryを使います。一般利用者はsystem directoryへ書き込めないため、
// rootで動かない場合だけ利用者ごとの場所へ切り替えます。
func UserDir() (string, bool) {
	return userDir(os.Geteuid(), os.Getenv, os.UserHomeDir)
}

func userDir(euid int, getenv func(string) string, homeDir func() (string, error)) (string, bool) {
	if euid == 0 {
		return "", false
	}
	// 仕様は、相対pathの$XDG_STATE_HOMEを無効として扱うよう定めています。
	if state := getenv("XDG_STATE_HOME"); filepath.IsAbs(state) {
		return state, true
	}
	home, err := homeDir()
	if err != nil || !filepath.IsAbs(home) {
		return "", false
	}
	return filepath.Join(home, ".local", "state"), true
}
