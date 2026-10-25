package failurearchive

import (
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	applicationpath "github.com/go-jappe/heterolog/pkg/path"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// durable用途
// ※logではない
type FileStore struct {
	FilePermanent           *os.File
	FileRetryable           *os.File
	Mu                      sync.Mutex
	FileNamePermanent       logpattern.FileNamePattern
	ActiveFilePathPermanent string // 現在使用中のファイルパス。閉じる際に必要。
	FileNameRetryable       logpattern.FileNamePattern
	ActiveFilePathRetryable string // 現在使用中のファイルパス。閉じる際に必要。
	Config                  FileArchiveConfigProvider
}

// ログファイルのパスを取得・更新古いパスを返却
func (fs *FileStore) UpdateFileRetryableFilePathAndGetOldpath(at time.Time) (string, error) {
	oldFilePath := fs.ActiveFilePathRetryable

	newFilePath, err := GetNewRetryableFilePath(&fs.FileNameRetryable, fs.Config, at)
	if err != nil {
		return "", err
	}

	fs.ActiveFilePathRetryable = newFilePath
	return oldFilePath, nil
}

func (fs *FileStore) UpdateFilePermanentFilePathAndGetOldpath(at time.Time) (string, error) {
	oldFilePath := fs.ActiveFilePathPermanent

	newFilePath, err := GetNewPermanentFilePath(&fs.FileNamePermanent, fs.Config, at)
	if err != nil {
		return "", err
	}

	fs.ActiveFilePathPermanent = newFilePath
	return oldFilePath, nil
}

func GetNewRetryableFilePath(
	f *logpattern.FileNamePattern,
	c FileArchiveConfigProvider,
	at time.Time,
) (string, error) {

	// 初回に解決した実体directoryをrotation後も使います。外部aliasの
	// 差し替えに追従すると、writerとguard/checkpointの所有directoryが分かれます。
	if f.Dir == "" {
		var err error
		f.Dir, err = GetFileArchiveDirectoryPath(c, c.DirCaseRetryable())
		if err != nil {
			return "", err
		}
	}

	return logpattern.ResolveFilePathAt(f, c, at)
}

func GetNewPermanentFilePath(
	f *logpattern.FileNamePattern,
	c FileArchiveConfigProvider,
	at time.Time,
) (string, error) {

	if f.Dir == "" {
		var err error
		f.Dir, err = GetFileArchiveDirectoryPath(c, c.DirCasePermanent())
		if err != nil {
			return "", err
		}
	}

	return logpattern.ResolveFilePathAt(f, c, at)
}

// GetFileArchiveDirectoryPath は、設定されたarchive rootのリンクだけを解決します。
// root配下の分類directoryはリンクを拒否してから作成し、targetは一度だけ付与します。
func GetFileArchiveDirectoryPath(c FileArchiveConfigProvider, terminalDir string) (string, error) {
	resolvedPath, err := resolveManagedFileArchiveDirectoryPath(c, terminalDir)
	if err != nil {
		return "", err
	}
	return fsstore.GetFileSystemStoreDirectoryPath(resolvedPath, "", "")
}

// 分類名は従来のJoin/Cleanで正規化してからroot相対へ戻します。
// 分類配下を先に実体解決すると、分類リンクを拒否する境界を失うため行いません。
func resolveManagedFileArchiveDirectoryPath(c ValidationLayoutConfigProvider, terminalDir string) (string, error) {
	root := resolveFileArchiveDirectoryPath(c, "")
	if terminalDir == "" {
		return pathidentity.ResolveDirectory(root)
	}
	configured := resolveFileArchiveDirectoryPath(c, terminalDir)
	relative, err := filepath.Rel(root, configured)
	if err != nil {
		return "", err
	}
	return pathidentity.ResolveAndValidateManagedDirectory(root, relative)
}

// resolveFileArchiveDirectoryPath はディレクトリを作成せず、実際に使用する
// failure archive directoryを設定値から解決します。
func resolveFileArchiveDirectoryPath(c ValidationLayoutConfigProvider, terminalDir string) string {
	return ResolveDirectoryPath(c.ArchivePath(), c.Target(), c.Dir(), terminalDir)
}

// ResolveDirectoryPath はfilesystemへ書き込まず、failure archiveが実際に
// 使用するdirectoryを設定値から解決します。
func ResolveDirectoryPath(archivePath, target, dir, terminalDir string) string {
	if target != "" {
		archivePath = applicationpath.BuildApplicationDataDirectoryPath(archivePath, target)
	}

	return filepath.Clean(filepath.Join(archivePath, dir, terminalDir))
}

func OpenFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		return nil, err
	}
	if err := validateArchiveAppendBoundary(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}
