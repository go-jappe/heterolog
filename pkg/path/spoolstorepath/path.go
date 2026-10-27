package storepath

import (
	"os"
	"path/filepath"
	"runtime"

	"github.com/go-jappe/heterolog/pkg/path/internal/statehome"
)

// DefaultSpoolDir returns the default directory that stores the spool of
// appName. On Linux, the BSDs, illumos, Solaris and AIX, it is
// /var/spool/<appName>, which follows the usual Linux hierarchy, when running
// as root, and otherwise <appName>/spool in the user's state directory
// ($XDG_STATE_HOME if it is an absolute path, or else ~/.local/state). When
// ~/.local/state would be used but the home directory is unknown, it returns
// the same location as for root.
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
