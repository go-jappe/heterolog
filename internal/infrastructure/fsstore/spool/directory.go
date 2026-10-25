package spool

import (
	"errors"
	"fmt"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	"github.com/go-jappe/heterolog/pkg/spool"
	"os"
	"path/filepath"
	"strings"
)

// DirectoryNameは通常ログと圧縮ログのspoolで共有するルートディレクトリ名です。
const DirectoryName = "spool"
const SpoolExt = ".hlogspool"

// NewRootDirの入力エラーです。呼び出し元はerrors.Isで判定します。
var (
	ErrEmptySpoolPath = errors.New("spool path is empty")
	ErrEmptyTarget    = errors.New("target is empty")
	ErrEmptyDirName   = errors.New("dir is empty")
)

// ディレクトリデータ転送型
type StateDir struct {
	path  string
	state spool.State
}

func (d StateDir) Path() string {
	return d.path
}

func (d StateDir) State() spool.State {
	return d.state
}

func (d StateDir) IsZero() bool {
	return d.path == "" || !d.state.IsValid()
}

// TimePartitionDirは、state配下の日時別保存先YYYY/MM/DD/HH/mmのパスを返します。
// ディレクトリの作成や存在確認は行いません。
func (d StateDir) TimePartitionDir(yyyy, mm, dd, hh, mi string) string {
	return filepath.Join(d.path, yyyy, mm, dd, hh, mi)
}

// 保存先ディレクトリを管理
type Dir struct {
	root string
}

func (d Dir) stateDir(s spool.State) StateDir {
	return StateDir{
		path:  filepath.Join(d.root, string(s)),
		state: s,
	}
}

func (d Dir) String() string {
	return d.root
}

func (d Dir) ActiveDir() StateDir     { return d.stateDir(spool.StateActive) }
func (d Dir) ProcessingDir() StateDir { return d.stateDir(spool.StateProcessing) }
func (d Dir) DoneDir() StateDir       { return d.stateDir(spool.StateDone) }
func (d Dir) DeadDir() StateDir       { return d.stateDir(spool.StateDead) }
func (d Dir) QuarantineDir() StateDir { return d.stateDir(spool.StateQuarantine) }

// 利用者基点spoolPath/targetのaliasを解決し、その下の固定spool layoutではlinkを拒否します。
// 既存state子directoryのlinkも拒否し、filesystemへの書込みは行いません。
// 空白だけ、または整理後に空になる入力には、filesystemを調べずに
// ErrEmptySpoolPath・ErrEmptyTarget・ErrEmptyDirNameを返します。
// 例:
//
//	spoolPath     = "/var/lib"
//	target        = "heterolog"
//	DirectoryName = "spool"
//	dirName       = "opensearch"
//
// 結果:
//
//	/var/lib/heterolog/spool/opensearch
func NewRootDir(spoolPath, target, dirName string) (Dir, error) {
	base := strings.TrimSpace(spoolPath)
	if base == "" {
		return Dir{}, ErrEmptySpoolPath
	}

	targetPart := sanitizePathPart(target)
	if targetPart == "" {
		return Dir{}, ErrEmptyTarget
	}

	dirPart := sanitizePathPart(dirName)
	if dirPart == "" {
		return Dir{}, ErrEmptyDirName
	}

	root, err := pathidentity.ResolveAndValidateManagedDirectory(
		filepath.Join(base, targetPart),
		filepath.Join(DirectoryName, dirPart),
	)
	if err != nil {
		return Dir{}, fmt.Errorf("resolve spool root directory: %w", err)
	}
	// 管理下stateを別の保存先へ振り分けるlinkは許可しません。未作成なら
	// 書込側が必要になった時点で作成し、ここではfilesystemを変更しません。
	for _, state := range []spool.State{
		spool.StateActive, spool.StateProcessing, spool.StateDone, spool.StateDead, spool.StateQuarantine,
	} {
		path := filepath.Join(root, string(state))
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return Dir{}, fmt.Errorf("inspect spool state directory %q: %w", path, err)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return Dir{}, &StateDirectoryError{State: state, Path: path}
		}
	}
	return Dir{root: root}, nil
}

// パス要素として危険な文字を置換する。
// target, dirName のような 1 要素分の文字列に使う。
func sanitizePathPart(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}

	replacer := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		":", "_",
		"*", "_",
		"?", "_",
		"\"", "_",
		"<", "_",
		">", "_",
		"|", "_",
		" ", "_",
	)
	s = replacer.Replace(s)
	s = strings.Trim(s, "._-")
	return s
}
