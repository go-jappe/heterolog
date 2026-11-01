package appendlog

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/cipherfile"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// 新しいファイルを開く
func OpenFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
}

// OpenFileは、active fileを追記用に開きます。Recipientがあれば、暗号化の新しいsegmentの
// headerを書き、以後のWriteStringWithRetryが暗号化して書くようにします。headerを書けなければ
// fileを閉じてerrorを返します。呼び出し元はMuを保持し、戻り値をFileに入れます。
func (fs *FileStore) OpenFile(path string) (*os.File, error) {
	file, err := OpenFile(path)
	if err != nil {
		return nil, err
	}
	if fs.Recipient == nil {
		fs.sealer = nil
		return file, nil
	}
	header, sealer, err := cipherfile.NewSealer(fs.Recipient)
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if _, err := file.Write(header); err != nil {
		return nil, errors.Join(fmt.Errorf("write the encryption header of %s: %w", path, err), file.Close())
	}
	fs.sealer = sealer
	return file, nil
}

// frameは、fileに書く1行を、暗号化が有効なら1つのchunkにします。
func (fs *FileStore) frame(line string) (string, error) {
	if fs.sealer == nil {
		return line, nil
	}
	chunk, err := fs.sealer.Seal([]byte(line))
	if err != nil {
		return "", err
	}
	return string(chunk), nil
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
	// Recipientは、fileを暗号化する公開鍵です。nilなら平文で書きます。sealerは、今開いている
	// fileのsegmentの暗号化の文脈で、OpenFileが差し替えます。
	Recipient *cipherfile.Recipient
	sealer    *cipherfile.Sealer
	OnError   logging.LoggerErrorHandler // 通知用の関数　内部エラー発生時にハンドル
	OnWarn    logging.LoggerHandlerFunc
	OnInfo    logging.LoggerHandlerFunc
	OnDebug   logging.LoggerHandlerFunc
	Tracer    pkgobserver.Tracer // observebility計装用
	Meter     pkgobserver.Meter  // observebility計装用
}

func (l *FileStore) Close() error {
	// 明示Close後には、未完了のrotationも再開しません。
	l.rotationPendingPath = ""
	if l.File != nil {
		return l.File.Close()
	}
	return nil
}
