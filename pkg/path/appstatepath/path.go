package appstatepath

import (
	"os"
	"path/filepath"
	"runtime"

	"github.com/go-jappe/heterolog/pkg/path/internal/statehome"
)

const ArchiveCheckpointDirectoryName = "archive-checkpoints"

// ArchiveCheckpointDirectory resolves, from the base path of the application
// state and the final target, the dedicated directory that appendlog archive
// checkpoints share.
func ArchiveCheckpointDirectory(basePath, target string) string {
	return filepath.Join(basePath, target, ArchiveCheckpointDirectoryName)
}

// DefaultAppStateDir returns the default directory that stores the state of
// appName. On Linux, the BSDs, illumos, Solaris and AIX, it is
// /var/lib/<appName> when running as root, and otherwise <appName> in the
// user's state directory ($XDG_STATE_HOME if it is an absolute path, or else
// ~/.local/state). When ~/.local/state would be used but the home directory is
// unknown, it returns the same location as for root.
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

// DefaultAppStateBaseDir returns the default base directory for storing spools
// and state. On Linux, the BSDs, illumos, Solaris and AIX, it is /var/lib when
// running as root, and otherwise the user's state directory ($XDG_STATE_HOME if
// it is an absolute path, or else ~/.local/state). When ~/.local/state would be
// used but the home directory is unknown, it returns /var/lib, the same as for
// root.
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
