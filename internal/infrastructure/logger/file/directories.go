package file

import "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"

// Directoryは、file loggerが使う一つのdirectoryと、その大きさの上限です。statusの報告に
// 使います。
type Directory struct {
	// Roleは、default・error・debugのいずれかです。
	Role string
	Path string
	// MaxDirSizeは、保持の設定の上限で、0なら上限なしです。
	MaxDirSize uint64
}

// Directoriesは、cfgのfile loggerが使うdirectoryを、使うroleの分だけ返します。pathは、
// LoggerSetと同じ規則で解決した実体のdirectoryです。解決できないroleは省きます。
func Directories(cfg FileLoggerConfigProvider) []Directory {
	limits := map[loggerType]appendlog.FilePurgeConfigProvider{
		DefaultLogger: cfg.Fsstore().Purge(),
		ErrorLogger:   cfg.Fsstore().ErrorPurge(),
		DebugLogger:   cfg.Fsstore().DebugPurge(),
	}
	return directoriesOf(cfg.Fsstore().Layout(), func(role loggerType) uint64 {
		if limit := limits[role]; limit != nil {
			return limit.MaxDirSize()
		}
		return 0
	})
}

// DeliveryDirectoriesは、cfgのDelivery file loggerが使うdirectoryを返します。保持の上限は、
// Deliveryのlifecycle runnerの設定（maxDirSize）です。
func DeliveryDirectories(cfg DeliveryFileLoggerConfigProvider, maxDirSize uint64) []Directory {
	return directoriesOf(cfg.Fsstore().Layout(), func(loggerType) uint64 { return maxDirSize })
}

func directoriesOf(layout appendlog.FsstoreLayoutConfigProvider, limitOf func(loggerType) uint64) []Directory {
	paths, err := resolveLoggerDirectoryPaths(layout)
	if err != nil {
		return nil
	}
	var directories []Directory
	for _, role := range []loggerType{DefaultLogger, ErrorLogger, DebugLogger} {
		path, ok := paths[role]
		if !ok {
			continue
		}
		if role == ErrorLogger && !layout.IsSeparateErrorLogsEnabled() {
			continue
		}
		if role == DebugLogger && !layout.IsSeparateDebugLogsEnabled() {
			continue
		}
		directories = append(directories, Directory{Role: string(role), Path: path, MaxDirSize: limitOf(role)})
	}
	return directories
}
