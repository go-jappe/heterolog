package statehome

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestUserDirUsesStateDirectoryOnlyForNonRootUsersWithAHome(t *testing.T) {
	// どのOSでも絶対pathになるよう、一時directoryの下の名前を使います。
	root := t.TempDir()
	home := filepath.Join(root, "home")
	state := filepath.Join(root, "state")
	homeDir := func() (string, error) { return home, nil }
	noHome := func() (string, error) { return "", errors.New("$HOME is not defined") }
	env := func(value string) func(string) string {
		return func(key string) string {
			if key == "XDG_STATE_HOME" {
				return value
			}
			return ""
		}
	}
	for _, tt := range []struct {
		name    string
		euid    int
		getenv  func(string) string
		homeDir func() (string, error)
		want    string
		wantOK  bool
	}{
		{name: "root keeps the system directory", euid: 0, getenv: env(state), homeDir: homeDir},
		{name: "XDG_STATE_HOME", euid: 1000, getenv: env(state), homeDir: homeDir, want: state, wantOK: true},
		{name: "default under the home directory", euid: 1000, getenv: env(""), homeDir: homeDir, want: filepath.Join(home, ".local", "state"), wantOK: true},
		{name: "relative XDG_STATE_HOME is ignored", euid: 1000, getenv: env(filepath.Join("relative", "state")), homeDir: homeDir, want: filepath.Join(home, ".local", "state"), wantOK: true},
		{name: "unknown home keeps the system directory", euid: 1000, getenv: env(""), homeDir: noHome},
		{name: "relative home keeps the system directory", euid: 1000, getenv: env(""), homeDir: func() (string, error) { return "home", nil }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := userDir(tt.euid, tt.getenv, tt.homeDir)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("userDir() = %q, %t; want %q, %t", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}
