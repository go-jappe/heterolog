package appendlog

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"os"
	"sync"
)

// 新しいファイルを開く
func OpenFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
}

// 実ファイルに出力
func ApendLine(file *os.File, line string) error {
	_, err := file.WriteString(line + "\n")
	return err
}

// ログファイルのパスを取得
func (fs *FileStore) UpdateFilePath() error {
	return fs.UpdateFilePathWithConfig(NewNameLogPatternConfigAdapter(fs.Config))
}

func (fs *FileStore) UpdateFilePathWithConfig(cfg logpattern.ConfigProvider) error {
	newFilePath, err := logpattern.ResolveFilePath(&fs.FileName, cfg)
	if err != nil {
		return err
	}

	fs.ActiveFilePath = newFilePath
	return nil
}

// log用途
// ※durableではない
type FileStore struct {
	File *os.File
	// Muはactive fileに関するFile、FileName、ActiveFilePathの整合性を保ち、
	// 同じstreamに対するwrite、rotation、closeを直列化します。
	Mu             sync.Mutex
	FileName       logpattern.FileNamePattern
	ActiveFilePath string // 現在使用中のファイルパス。閉じる際に必要。
	// rotationPendingPathは、旧handleをCloseで手放したが新fileのopenが
	// 未完了のrotation元pathです。Closeがerrorを返した場合も設定します。
	// Muで保護し、再開時の二重Closeを避けます。
	rotationPendingPath string
	Config              FileStoreConfigProvider
	OnError             logging.LoggerErrorHandler // 通知用の関数　内部エラー発生時にハンドル
	OnWarn              logging.LoggerHandlerFunc
	OnInfo              logging.LoggerHandlerFunc
	OnDebug             logging.LoggerHandlerFunc
	Tracer              pkgobserver.Tracer // observebility計装用
	Meter               pkgobserver.Meter  // observebility計装用
}

func (l *FileStore) Close() error {
	// 明示Close後には、未完了のrotationも再開しません。
	l.rotationPendingPath = ""
	if l.File != nil {
		return l.File.Close()
	}
	return nil
}
