package file

import (
	"fmt"
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// loggerDirectoryPathsは構築時の実体directoryを保持します。lazy roleのopen、
// raw回復、upload保護も同じsnapshotを使い、外部aliasの差し替えには追従しません。
type loggerDirectoryPaths map[loggerType]string

// ErrorDebugDirectorySharingErrorは、分離したerrorとdebugのroleが同じdirectoryを使い、
// defaultのroleだけが別のdirectoryを使う設定を拒否したことを表します。
// SharedDirectoryとDefaultDirectoryは、pathidentity.ComparisonKeyで正規化した実体directoryです。
type ErrorDebugDirectorySharingError struct {
	SharedDirectory  string
	DefaultDirectory string
}

func (e *ErrorDebugDirectorySharingError) Error() string {
	return fmt.Sprintf(
		"file logger roles %q and %q cannot share directory %q while role %q uses a different directory %q",
		ErrorLogger, DebugLogger, e.SharedDirectory, DefaultLogger, e.DefaultDirectory,
	)
}

func configuredLoggerDirectory(layout appendlog.FsstoreLayoutConfigProvider, role loggerType) string {
	directory := layout.LogDir()
	switch role {
	case ErrorLogger:
		directory = layout.ErrorLogDir()
	case DebugLogger:
		directory = layout.DebugLogDir()
	}
	return filepath.Join(layout.LogPath(), layout.Target(), directory)
}

// 未使用roleは既存rawの回復用にだけ解決します。保存先が不正でも未使用設定を
// constructorの失敗理由にはせず、そのroleの過去fileを回復対象から外します。
func resolveLoggerDirectoryPaths(layout appendlog.FsstoreLayoutConfigProvider) (loggerDirectoryPaths, error) {
	directories := make(loggerDirectoryPaths, 3)
	for _, role := range []loggerType{DefaultLogger, ErrorLogger, DebugLogger} {
		configured := configuredLoggerDirectory(layout, role)
		directory, err := pathidentity.ResolveDirectory(configured)
		if err != nil {
			required := role == DefaultLogger ||
				(role == ErrorLogger && layout.IsSeparateErrorLogsEnabled()) ||
				(role == DebugLogger && layout.IsSeparateDebugLogsEnabled())
			if required {
				return nil, fmt.Errorf("resolve file logger %s directory %q: %w", role, configured, err)
			}
			continue
		}
		directories[role] = directory
	}
	if layout.IsSeparateErrorLogsEnabled() && layout.IsSeparateDebugLogsEnabled() {
		defaultDir := pathidentity.ComparisonKey(directories[DefaultLogger])
		errorDir := pathidentity.ComparisonKey(directories[ErrorLogger])
		debugDir := pathidentity.ComparisonKey(directories[DebugLogger])
		if errorDir == debugDir && errorDir != defaultDir {
			return nil, &ErrorDebugDirectorySharingError{SharedDirectory: errorDir, DefaultDirectory: defaultDir}
		}
	}
	return directories, nil
}
