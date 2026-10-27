package logpath

import (
	"os"
	"path/filepath"
	"runtime"

	"github.com/go-jappe/heterolog/pkg/path/internal/statehome"
)

// DefaultLogDir returns the default directory that stores the logs of appName.
// On Linux, the BSDs, illumos, Solaris and AIX, it is /var/log/<appName> when
// running as root, and otherwise <appName>/logs in the user's state directory
// ($XDG_STATE_HOME if it is an absolute path, or else ~/.local/state). When
// ~/.local/state would be used but the home directory is unknown, it returns
// the same location as for root.
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

// DefaultLogBaseDir returns the default base directory for storing logs. On
// Linux, the BSDs, illumos, Solaris and AIX, it is /var/log when running as
// root, and otherwise the user's state directory ($XDG_STATE_HOME if it is an
// absolute path, or else ~/.local/state). When ~/.local/state would be used but
// the home directory is unknown, it returns /var/log, the same as for root.
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
