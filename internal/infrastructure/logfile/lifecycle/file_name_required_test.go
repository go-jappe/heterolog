package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFileSeriesManagerRequiresExplicitFileNameBeforeFilesystemOperations(t *testing.T) {
	cases := []struct {
		name  string
		field string
		value string
	}{
		{name: "omitted file name", field: "all"},
		{name: "missing directory", field: "directory"},
		{name: "missing target", field: "target"},
		{name: "missing extension", field: "extension"},
		{name: "blank directory", field: "directory", value: " \t\r\n"},
		{name: "blank target", field: "target", value: " \t\r\n"},
		{name: "blank extension", field: "extension", value: " \t\r\n"},
		{name: "unicode blank directory", field: "directory", value: "\u2003"},
		{name: "unicode blank target", field: "target", value: "\u2003"},
		{name: "unicode blank extension", field: "extension", value: "\u2003"},
		{name: "dot extension", field: "extension", value: "."},
	}
	operations := []struct {
		name string
		run  func(*FileSeriesManager) error
	}{
		{name: "manager validation", run: func(manager *FileSeriesManager) error { return manager.validate() }},
		{name: "new lifecycle", run: func(manager *FileSeriesManager) error {
			_, err := NewDirectoryLifecycle(manager)
			return err
		}},
		{name: "manager run", run: func(manager *FileSeriesManager) error {
			_, err := manager.RunOnce(context.Background())
			return err
		}},
		{name: "lifecycle run", run: func(manager *FileSeriesManager) error {
			lifecycle := &DirectoryLifecycle{fileSeriesManagers: []*FileSeriesManager{manager}}
			_, err := lifecycle.RunOnce(context.Background())
			return err
		}},
		{name: "lifecycle reconcile", run: func(manager *FileSeriesManager) error {
			lifecycle := &DirectoryLifecycle{fileSeriesManagers: []*FileSeriesManager{manager}}
			_, err := lifecycle.ReconcileObjectStorageUploadDestinations(context.Background())
			return err
		}},
		{name: "lifecycle directory path", run: func(manager *FileSeriesManager) error {
			lifecycle := &DirectoryLifecycle{fileSeriesManagers: []*FileSeriesManager{manager}}
			_, err := lifecycle.DirectoryPath()
			return err
		}},
	}
	for _, tc := range cases {
		for _, operation := range operations {
			t.Run(tc.name+"/"+operation.name, func(t *testing.T) {
				cfg := configuredCheckpointFactoryTestConfig(t)
				pattern := logpattern.FileNamePattern{
					Dir: filepath.Join(cfg.FsstoreConfig.Layout.LogPath, "explicit"), Target: "explicit", Extension: "log",
				}
				switch tc.field {
				case "all":
					pattern = logpattern.FileNamePattern{}
				case "directory":
					pattern.Dir = tc.value
				case "target":
					pattern.Target = tc.value
				case "extension":
					pattern.Extension = tc.value
				}
				manager := NewFileSeriesManager(FileSeriesManagerOptions{
					Config: appendlog.NewConfigAdapter(cfg.FsstoreConfig), FileName: pattern,
				})
				require.NotNil(t, manager, "constructor retains its existing non-error signature")
				require.ErrorIs(t, operation.run(manager), ErrInvalidFileName)
				require.Equal(t, pattern, manager.FileNamePattern(), "valid Config must not fill missing pattern fields")
				entries, err := os.ReadDir(cfg.FsstoreConfig.Layout.LogPath)
				require.NoError(t, err)
				require.Empty(t, entries, "invalid FileName must not create log or checkpoint directories or files")
			})
		}
	}
}

func TestFileSeriesManagerKeepsExplicitFileNameAndDoesNotCreateDirectory(t *testing.T) {
	for _, prefix := range []string{"", "audit_"} {
		t.Run("prefix="+prefix, func(t *testing.T) {
			cfg := configuredCheckpointFactoryTestConfig(t)
			pattern := logpattern.FileNamePattern{
				Dir:       filepath.Join(cfg.FsstoreConfig.Layout.LogPath, "explicit", "not-created"),
				Prefix:    prefix,
				Target:    "explicit-target",
				Extension: "jsonl",
			}
			manager := NewFileSeriesManager(FileSeriesManagerOptions{
				Config: appendlog.NewConfigAdapter(cfg.FsstoreConfig), FileName: pattern,
			})
			for range 2 {
				effective, err := manager.fileNamePattern()
				require.NoError(t, err)
				require.Equal(t, pattern, effective, "Config's ordinary log pattern must not overwrite explicit fields")
				require.Equal(t, pattern, manager.FileNamePattern())
				require.True(t, manager.hasUncompressedExtension(filepath.Join(pattern.Dir, prefix+pattern.Target+"_20260821_1.jsonl")))
				require.False(t, manager.hasUncompressedExtension(filepath.Join(pattern.Dir, prefix+pattern.Target+"_20260821_1.log")))
				require.NoError(t, manager.validate())
				lifecycle, err := NewDirectoryLifecycle(manager)
				require.NoError(t, err)
				require.NotNil(t, lifecycle)
				_, err = lifecycle.ReconcileObjectStorageUploadDestinations(context.Background())
				require.NoError(t, err)
				_, err = manager.RunOnce(context.Background())
				require.NoError(t, err)
				require.Equal(t, pattern, manager.FileNamePattern())
				entries, err := os.ReadDir(cfg.FsstoreConfig.Layout.LogPath)
				require.NoError(t, err)
				require.Empty(t, entries, "manager must not create either explicit or Config-derived directories")
			}
		})
	}
}

func TestNewLogFileLifecyclesSuppliesCompleteFileNamePatterns(t *testing.T) {
	cfg := configuredCheckpointFactoryTestConfig(t)
	cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{}
	cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{}
	layout := &cfg.FsstoreConfig.Layout
	layout.IsSeparateErrorLogsEnabled = true
	layout.ErrorLogDir = "errors"
	layout.SeparateErrorLogsPrefix = "error_"
	layout.IsSeparateDebugLogsEnabled = true
	layout.IsSeparateDebugLogDirEnabled = true
	layout.DebugLogDir = "debug"
	layout.SeparateDebugLogsPrefix = "debug_"
	lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.NoError(t, err)
	require.Len(t, lifecycles, 3)
	want := map[string]logpattern.FileNamePattern{
		"":       {Dir: filepath.Join(layout.LogPath, layout.Target, layout.LogDir), Target: layout.Target, Extension: "log"},
		"error_": {Dir: filepath.Join(layout.LogPath, layout.Target, layout.ErrorLogDir), Prefix: "error_", Target: layout.Target, Extension: "log"},
		"debug_": {Dir: filepath.Join(layout.LogPath, layout.Target, layout.DebugLogDir), Prefix: "debug_", Target: layout.Target, Extension: "log"},
	}
	for _, lifecycle := range lifecycles {
		require.Len(t, lifecycle.fileSeriesManagers, 1)
		manager := lifecycle.fileSeriesManagers[0]
		pattern := manager.FileNamePattern()
		require.Contains(t, want, pattern.Prefix)
		require.Equal(t, want[pattern.Prefix], pattern)
		effective, err := manager.fileNamePattern()
		require.NoError(t, err)
		require.Equal(t, pattern, effective)
		delete(want, pattern.Prefix)
	}
	require.Empty(t, want)
}
