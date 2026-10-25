package config

import (
	"os"
	"runtime"
	"testing"
)

// TestLoadConfigDefaultsStorageBasesByUserは、保存先の基点を省略した場合の既定値を検証します。
// Linuxでrootで動かない場合は、一般利用者が書き込めない/var/logなどではなく、利用者の
// state directory（XDG_STATE_HOME）を使います。rootで動く場合は/var/logと/var/libのままです。
func TestLoadConfigDefaultsStorageBasesByUser(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the user state directory default is verified on Linux")
	}
	state := t.TempDir()
	workDirectory := t.TempDir()
	restoreProcessEnvAfterTest(t)
	clearProcessEnvForTest(t)
	t.Chdir(workDirectory)
	if err := os.Setenv("XDG_STATE_HOME", state); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() without storage settings: %v", err)
	}
	wantLog, wantSpoolAndState := state, state
	if os.Geteuid() == 0 {
		wantLog, wantSpoolAndState = "/var/log", "/var/lib"
	}
	if got := cfg.FsstoreConfig.LogPath; got != wantLog {
		t.Errorf("default HETEROLOG_SYSTEM_LOG_DIR = %q, want %q", got, wantLog)
	}
	if got := cfg.FsstoreConfig.SpoolPath; got != wantSpoolAndState {
		t.Errorf("default HETEROLOG_SYSTEM_LIB_DIR = %q, want %q", got, wantSpoolAndState)
	}
	if got := cfg.FsstoreConfig.AppStatePath; got != wantSpoolAndState {
		t.Errorf("default HETEROLOG_APP_STATE_DIR = %q, want %q", got, wantSpoolAndState)
	}
}
