package file

import "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"

// ロガータイプに応じてディレクトリを分けて返却
func (f *FileLogger) getLogDir() string {
	switch f.logger {
	case ErrorLogger:
		return f.config.Fsstore().Layout().ErrorLogDir()
	case DefaultLogger:
		return f.config.Fsstore().Layout().LogDir()
	case DebugLogger:
		return f.config.Fsstore().Layout().DebugLogDir()
	default:
		return f.config.Fsstore().Layout().LogDir()
	}
}

func (f *FileLogger) getPrefixIfEnabled() string {
	if f.logger == ErrorLogger && f.config.Fsstore().Layout().IsSeparateErrorLogsEnabled() {
		return f.getPrefix()
	}

	if f.logger == DebugLogger && f.config.Fsstore().Layout().IsSeparateDebugLogsEnabled() {
		return f.getPrefix()
	}
	return ""
}

// 必要に応じてログファイル名にerror_ debug_などにつける接頭辞を返却する
func (f *FileLogger) getPrefix() string {
	// エラーログ、デバッグログは接頭辞を付与
	prefix := ""
	if f.logger == ErrorLogger {
		prefix = f.config.Fsstore().Layout().SeparateErrorLogsPrefix()
	} else if f.logger == DebugLogger {
		prefix = f.config.Fsstore().Layout().SeparateDebugLogsPrefix()
	}
	return prefix
}

func (f *FileLogger) GetFileSystemStoreDirectoryPath() (string, error) {
	if directory := f.directoryPaths[f.logger]; directory != "" {
		return directory, nil
	}

	return fsstore.GetFileSystemStoreDirectoryPath(
		f.config.Fsstore().Layout().LogPath(),
		f.config.Fsstore().Layout().Target(),
		f.getLogDir())
}
