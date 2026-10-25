package logpath

import (
	"os"
	"path/filepath"
	"runtime"

	"github.com/go-jappe/heterolog/pkg/path/internal/statehome"
)

// DefaultLogDirは、appNameのログを保存する既定のdirectoryを返します。Linuxなどのunix系OSでは、
// rootで動く場合は/var/log/<appName>、それ以外は利用者のstate directory
// （$XDG_STATE_HOME、未設定なら~/.local/state）の<appName>/logsです。home directoryが
// 分からない場合はrootと同じ場所を返します。
func DefaultLogDir(appName string) string {

	switch runtime.GOOS {

	case "linux", "freebsd", "openbsd", "netbsd", "dragonfly", "illumos", "solaris", "aix":
		if state, ok := statehome.UserDir(); ok {
			return filepath.Join(state, appName, "logs")
		}
		return filepath.Join("/var/log", appName)

	case "darwin":
		home, _ := os.UserHomeDir()
		return filepath.Join(home, "Library", "Logs", appName)

	case "windows":
		base := os.Getenv("LOCALAPPDATA")
		if base == "" {
			base = os.Getenv("APPDATA")
		}
		if base == "" {
			home, _ := os.UserHomeDir()
			base = filepath.Join(home, "AppData", "Local")
		}
		return filepath.Join(base, appName, "logs")

	case "plan9":
		home, _ := os.UserHomeDir()
		return filepath.Join(home, appName, "log")

	case "js", "wasip1":
		return ""

	default:
		home, _ := os.UserHomeDir()
		return filepath.Join(home, appName, "logs")
	}
}

// DefaultLogBaseDirは、ログの保存先の既定の基点directoryを返します。Linuxなどのunix系OSでは、
// rootで動く場合は/var/log、それ以外は利用者のstate directory（$XDG_STATE_HOME、未設定なら
// ~/.local/state）です。home directoryが分からない場合はrootと同じ/var/logを返します。
func DefaultLogBaseDir() string {
	switch runtime.GOOS {
	case "linux", "freebsd", "openbsd", "netbsd", "dragonfly", "illumos", "solaris", "aix":
		if state, ok := statehome.UserDir(); ok {
			return state
		}
		return "/var/log"

	case "darwin":
		home, _ := os.UserHomeDir()
		if home == "" {
			return ""
		}
		return filepath.Join(home, "Library", "Logs")

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
		return ""
	}
}
