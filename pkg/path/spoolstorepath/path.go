package storepath

import (
	"os"
	"path/filepath"
	"runtime"

	"github.com/go-jappe/heterolog/pkg/path/internal/statehome"
)

// DefaultSpoolDirは、appNameのspoolを保存する既定のdirectoryを返します。Linuxなどのunix系OSでは、
// rootで動く場合は通常のLinuxの階層に準じた/var/spool/<appName>、それ以外は利用者のstate
// directory（$XDG_STATE_HOME、未設定なら~/.local/state）の<appName>/spoolです。home directoryが
// 分からない場合はrootと同じ場所を返します。
func DefaultSpoolDir(appName string) string {
	switch runtime.GOOS {

	case "linux", "freebsd", "openbsd", "netbsd", "dragonfly", "illumos", "solaris", "aix":
		if state, ok := statehome.UserDir(); ok {
			return filepath.Join(state, appName, "spool")
		}
		return filepath.Join("/var/spool", appName)

	case "darwin":
		home, _ := os.UserHomeDir()
		return filepath.Join(home, "Library", "Application Support", appName, "spool")

	case "windows":
		base := os.Getenv("LOCALAPPDATA")
		if base == "" {
			base = os.Getenv("APPDATA")
		}
		if base == "" {
			home, _ := os.UserHomeDir()
			base = filepath.Join(home, "AppData", "Local")
		}
		return filepath.Join(base, appName, "spool")

	case "plan9":
		home, _ := os.UserHomeDir()
		return filepath.Join(home, appName, "spool")

	case "js", "wasip1":
		return ""

	default:
		home, _ := os.UserHomeDir()
		return filepath.Join(home, "."+appName, "spool")
	}
}
