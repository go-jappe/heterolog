package mappedvalidation

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestMappedFileRolePurgeUsesResolvedSharedDirectory(t *testing.T) {
	root := t.TempDir()
	physical := filepath.Join(root, "app", "default")
	if err := os.MkdirAll(physical, 0o750); err != nil {
		t.Fatal(err)
	}
	testutil.Symlink(t, physical, filepath.Join(root, "app", "error-alias"))
	testutil.Symlink(t, physical, filepath.Join(root, "app", "debug-alias"))
	for _, source := range []string{"logger", "async logger", "delivery logger"} {
		for _, internal := range []bool{false, true} {
			label := source + " file"
			if internal {
				label = source + " internal log file"
			}
			for _, role := range []string{"error", "debug"} {
				for _, tc := range []struct {
					name string
					edit func(*pkgfsstore.FilePurgeConfig)
					want error
				}{
					{name: "zero capacity ignored", edit: func(p *pkgfsstore.FilePurgeConfig) { p.MaxDirSize = 0 }},
					{name: "small capacity ignored", edit: func(p *pkgfsstore.FilePurgeConfig) { p.MaxDirSize = 1 }},
					{name: "different capacity ignored", edit: func(p *pkgfsstore.FilePurgeConfig) { p.MaxDirSize = 60 << 20 }},
					{name: "invalid age retained", edit: func(p *pkgfsstore.FilePurgeConfig) { p.MaxFileAge = 0 }, want: infrafsstore.ErrInvalidMaxFileAge},
					{name: "invalid backups retained", edit: func(p *pkgfsstore.FilePurgeConfig) { p.MaxFileBackups = 0 }, want: infrafsstore.ErrInvalidMaxFileBackups},
				} {
					t.Run(label+"/"+role+"/"+tc.name, func(t *testing.T) {
						cfg := mappedRolePurgeFileConfig()
						cfg.FsstoreConfig.Layout.LogPath = root
						cfg.FsstoreConfig.Layout.ErrorLogDir = "error-alias"
						cfg.FsstoreConfig.Layout.DebugLogDir = "debug-alias"
						purge := cfg.FsstoreConfig.Lifecycle.ErrorPurge
						if role == "debug" {
							purge = cfg.FsstoreConfig.Lifecycle.DebugPurge
						}
						tc.edit(purge)
						before := *purge
						err := validateMappedRolePurgeDestination(source, internal, cfg)
						if tc.want == nil && err != nil {
							t.Fatalf("shared alias validation: %v", err)
						}
						if tc.want != nil {
							requirePurgeConfigError(t, err, label, appendlog.PurgeRole(role), tc.want)
						}
						if *purge != before {
							t.Fatalf("validation changed raw purge config: got %+v, want %+v", *purge, before)
						}
					})
				}
			}
		}
	}
	entries, err := os.ReadDir(physical)
	if err != nil || len(entries) != 0 {
		t.Fatalf("validation wrote to shared directory: entries=%v, err=%v", entries, err)
	}
}

func TestMappedFileRolePurgeSharedAliasUsesInvalidNormalCapacity(t *testing.T) {
	root := t.TempDir()
	physical := filepath.Join(root, "app", "default")
	if err := os.MkdirAll(physical, 0o750); err != nil {
		t.Fatal(err)
	}
	testutil.Symlink(t, physical, filepath.Join(root, "app", "alias"))
	cfg := mappedRolePurgeFileConfig()
	cfg.FsstoreConfig.Layout.LogPath = root
	cfg.FsstoreConfig.Layout.ErrorLogDir = "alias"
	cfg.FsstoreConfig.Layout.DebugLogDir = "alias"
	cfg.FsstoreConfig.Lifecycle.MaxDirSize = 0
	err := validateMappedLogFilePurgeConfigs("logger file", appendlog.NewConfigAdapter(cfg.FsstoreConfig))
	// 個別の容量上限は正しくても、通常ログと共有するdirectoryでは通常ログの不正な上限で検証します。
	for _, role := range []appendlog.PurgeRole{appendlog.PurgeRoleError, appendlog.PurgeRoleDebug} {
		requirePurgeConfigError(t, err, "logger file", role, infrafsstore.ErrInvalidMaxDirSize)
	}
}

func TestMappedFileRolePurgeDoesNotResolveUnusedDirectories(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "app"), 0o750); err != nil {
		t.Fatal(err)
	}
	testutil.Symlink(t, filepath.Join(root, "missing"), filepath.Join(root, "app", "broken"))
	for _, unused := range []string{"error", "debug", "both"} {
		t.Run(unused, func(t *testing.T) {
			cfg := mappedRolePurgeFileConfig()
			cfg.FsstoreConfig.Layout.LogPath = root
			if unused == "error" || unused == "both" {
				cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
				cfg.FsstoreConfig.Layout.ErrorLogDir = "broken"
			}
			if unused == "debug" || unused == "both" {
				cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
				cfg.FsstoreConfig.Layout.DebugLogDir = "broken"
			}
			if err := validateMappedLogFilePurgeConfigs("logger file", appendlog.NewConfigAdapter(cfg.FsstoreConfig)); err != nil {
				t.Fatalf("unused directory caused validation failure: %v", err)
			}
		})
	}
	cfg := mappedRolePurgeFileConfig()
	cfg.FsstoreConfig.Layout.LogPath = root
	cfg.FsstoreConfig.Layout.ErrorLogDir = "broken"
	err := validateMappedLogFilePurgeConfigs("logger file", appendlog.NewConfigAdapter(cfg.FsstoreConfig))
	configErr := requireConfigError(t, err, "logger file", ConfigSectionLifecycle, nil)
	var resolutionErr *LogDirectoryResolutionError
	if !errors.As(configErr, &resolutionErr) || resolutionErr.Role != "error" ||
		resolutionErr.Directory != filepath.Join(root, "app", "broken") || resolutionErr.Err == nil {
		t.Fatalf("active dangling directory resolution error = %v, want error log directory resolution error", err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "app"))
	if err != nil || len(entries) != 1 || entries[0].Name() != "broken" {
		t.Fatalf("validation created directories: entries=%v, err=%v", entries, err)
	}
}
