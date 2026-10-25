package fsstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// CountPreservedFilesはroot直下の.preservedに残っている保全対象を数えます。
// renameとコピーの保全名を区別せず、管理者が確認すべき残存件数として返します。
// 隔離したリンク・ディレクトリ・非通常ファイルも直下の項目ごとに1件と数え、
// リンク先やディレクトリの中身はたどりません。コピー作成中の専用一時通常ファイルは
// 数えません。保全先がなければ0を返し、作成はしません。
// 情報取得失敗は、件数と最初の原因をまとめてエラーにします。
// ファイルの内容を読み取ったり、削除・再送したりはしません。
func CountPreservedFiles(ctx context.Context, rootPath string) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if strings.TrimSpace(rootPath) == "" {
		return 0, errors.New("preserved file root must not be empty")
	}
	directory := filepath.Join(rootPath, PreservedDirectoryName)
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("inspect preserved file directory %q: %w", directory, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return 0, &DirectoryObstructionError{Path: directory, message: fmt.Sprintf("preserved file path %q is not a real directory", directory)}
	}
	// Windows junctionなど、ModeSymlinkだけでは表現されないリンクも拒否します。
	if _, err := pathidentity.ResolveAndValidateManagedDirectory(rootPath, PreservedDirectoryName); err != nil {
		return 0, fmt.Errorf("inspect preserved file directory %q: %w", directory, err)
	}
	entries, err := ReadDirectoryEntries(directory)
	if err != nil {
		return 0, fmt.Errorf("list preserved files in %q: %w", directory, err)
	}
	count, invalid := 0, 0
	var firstErr error
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return count, errors.Join(err, firstErr)
		}
		path := filepath.Join(directory, entry.Name())
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			invalid++
			if firstErr == nil {
				firstErr = fmt.Errorf("%q: %w", path, err)
			}
			continue
		}
		if info.Mode().IsRegular() && strings.HasPrefix(entry.Name(), ".preserve-") && strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		count++
	}
	if invalid != 0 {
		return count, fmt.Errorf("%d invalid preserved file entries in %q; first: %w", invalid, directory, firstErr)
	}
	return count, nil
}
