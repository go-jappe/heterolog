package file

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

const (
	defaultLoggerSetDirectoryScopeLabel         directorylease.ScopeLabel = "file.logger_set"
	defaultDeliveryLoggerSetDirectoryScopeLabel directorylease.ScopeLabel = "delivery_file.logger_set"
)

// ErrLoggerRolesShareFilesは、同じdirectoryを使うroleが同じfile name prefixを持ち、
// 別々のFileLoggerが同じfileを開く設定を表します。大小文字を区別しないdirectoryでは、
// 大小文字だけが異なるprefixも同じfileを指します。
var ErrLoggerRolesShareFiles = errors.New("file logger roles use the same directory and file prefix")

// LoggerRolesShareFilesErrorは、ErrLoggerRolesShareFilesの拒否で同じfileを指したroleを示します。
// Rolesは先に設定を確認したroleから順に保持し、Directoryは後のroleの実体directory、
// Prefixは後のroleのfile name prefixです。errors.IsでErrLoggerRolesShareFilesと判別できます。
type LoggerRolesShareFilesError struct {
	Roles     [2]string
	Directory string
	Prefix    string
}

func (e *LoggerRolesShareFilesError) Error() string {
	return fmt.Sprintf(
		"file logger roles %q and %q use the same directory %q and file prefix %q: %v",
		e.Roles[0], e.Roles[1], e.Directory, e.Prefix, ErrLoggerRolesShareFiles,
	)
}

func (e *LoggerRolesShareFilesError) Unwrap() error { return ErrLoggerRolesShareFiles }

// acquireLoggerSetDirectoryLeaseは、一つのLoggerSetが使用し得るdefault/error/debug
// roleの保存先を、fileを開く前にまとめて予約します。同じdirectoryのroleは
// file name prefixで共有し、同一取得要求内の共有として扱われます。
// 一方、別pathに分けたroleのdirectoryが親子関係になる設定は、
// lifecycleの所有範囲が重なるためdirectorylease側で拒否されます。
func acquireLoggerSetDirectoryLease(
	scopeLabel directorylease.ScopeLabel,
	layout appendlog.FsstoreLayoutConfigProvider,
	directories loggerDirectoryPaths,
) (*directorylease.Lease, error) {
	type directoryRole struct {
		name   string
		path   string
		prefix string
	}

	roles := []directoryRole{
		{
			name: "default",
			path: directories[DefaultLogger],
		},
	}
	if layout.IsSeparateErrorLogsEnabled() {
		roles = append(roles, directoryRole{
			name:   "error",
			path:   directories[ErrorLogger],
			prefix: layout.SeparateErrorLogsPrefix(),
		})
	}
	if layout.IsSeparateDebugLogsEnabled() {
		roles = append(roles, directoryRole{
			name:   "debug",
			path:   directories[DebugLogger],
			prefix: layout.SeparateDebugLogsPrefix(),
		})
	}

	paths := make([]string, 0, len(roles))
	for _, role := range roles {
		paths = append(paths, role.path)
	}

	// 同じdirectoryを共有するroleはprefixで物理fileを分離しなければなりません。
	// prefixも同じなら、別FileLoggerが同じfileを別々のlockで開くため拒否します。
	for currentIndex, current := range roles {
		// directoryだけでなくprefixも含む比較keyにし、WindowsでErr_/err_が
		// 同じfileを指す場合も拒否します。実際のfile名は変更しません。
		currentSeries := pathidentity.ComparisonKey(filepath.Join(current.path, current.prefix+layout.Target()))
		for previousIndex := 0; previousIndex < currentIndex; previousIndex++ {
			previous := roles[previousIndex]
			previousSeries := pathidentity.ComparisonKey(filepath.Join(previous.path, previous.prefix+layout.Target()))
			if currentSeries == previousSeries {
				return nil, &LoggerRolesShareFilesError{
					Roles:     [2]string{previous.name, current.name},
					Directory: current.path,
					Prefix:    current.prefix,
				}
			}
		}
	}

	lease, err := directorylease.Acquire(scopeLabel, paths...)
	if err != nil {
		return nil, fmt.Errorf("acquire file logger directory lease: %w", err)
	}
	return lease, nil
}

func deliveryFileLoggerDirectoryScopeLabel(role loggerType) directorylease.ScopeLabel {
	return directorylease.ScopeLabel("delivery_file_logger." + string(role))
}
