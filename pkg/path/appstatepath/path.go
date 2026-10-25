package appstatepath

import (
	"os"
	"path/filepath"
	"runtime"

	"github.com/go-jappe/heterolog/pkg/path/internal/statehome"
)

const ArchiveCheckpointDirectoryName = "archive-checkpoints"

// ArchiveCheckpointDirectoryはapplication stateのbase pathと最終targetから、
// appendlog archive checkpointが共有する専用directoryを解決します。
func ArchiveCheckpointDirectory(basePath, target string) string {
	return filepath.Join(basePath, target, ArchiveCheckpointDirectoryName)
}

// DefaultAppStateDirは、appNameの状態を保存する既定のdirectoryを返します。Linuxなどのunix系OSでは、
// rootで動く場合は/var/lib/<appName>、それ以外は利用者のstate directory
// （$XDG_STATE_HOME、未設定なら~/.local/state）の<appName>です。home directoryが
// 分からない場合はrootと同じ場所を返します。
func DefaultAppStateDir(appName string) string {
	switch runtime.GOOS {

	case "linux", "freebsd", "openbsd", "netbsd", "dragonfly", "illumos", "solaris", "aix":
		if state, ok := statehome.UserDir(); ok {
			return filepath.Join(state, appName)
		}
		return filepath.Join("/var/lib", appName)

	case "darwin":
		home, _ := os.UserHomeDir()
		return filepath.Join(home, "Library", "Application Support", appName)

	case "windows":
		base := os.Getenv("LOCALAPPDATA")
		if base == "" {
			base = os.Getenv("APPDATA")
		}
		if base == "" {
			home, _ := os.UserHomeDir()
			base = filepath.Join(home, "AppData", "Local")
		}
		return filepath.Join(base, appName)

	case "plan9":
		home, _ := os.UserHomeDir()
		return filepath.Join(home, appName, "lib")

	case "js", "wasip1":
		return ""

	default:
		home, _ := os.UserHomeDir()
		return filepath.Join(home, ".local", "state", appName)
	}
}

// DefaultAppStateBaseDirは、spoolと状態の保存先の既定の基点directoryを返します。Linuxなどの
// unix系OSでは、rootで動く場合は/var/lib、それ以外は利用者のstate directory
// （$XDG_STATE_HOME、未設定なら~/.local/state）です。home directoryが分からない場合は
// rootと同じ/var/libを返します。
func DefaultAppStateBaseDir() string {
	switch runtime.GOOS {
	case "linux", "freebsd", "openbsd", "netbsd", "dragonfly", "illumos", "solaris", "aix":
		if state, ok := statehome.UserDir(); ok {
			return state
		}
		return "/var/lib"

	case "darwin":
		home, _ := os.UserHomeDir()
		if home == "" {
			return ""
		}
		return filepath.Join(home, "Library", "Application Support")

	case "windows":
		if base := os.Getenv("LOCALAPPDATA"); base != "" {
			return base
		}
		if base := os.Getenv("APPDATA"); base != "" {
			return base
		}

		home, _ := os.UserHomeDir()
		if home == "" {
			return ""
		}
		return filepath.Join(home, "AppData", "Local")

	case "js", "wasip1", "plan9":
		return ""

	default:
		home, _ := os.UserHomeDir()
		if home == "" {
			return ""
		}
		return filepath.Join(home, ".local", "state")
	}
}
