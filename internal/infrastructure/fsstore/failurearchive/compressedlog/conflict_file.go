package compressedlogarchive

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"

	fsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

const (
	preservedConflictPrefix   = ".compressed-log-conflict-"
	preservedConflictSuffix   = ".preserved"
	preservedConflictAttempts = 16
)

// preserveConflictingArchiveFileは元の分類directoryにある名前を空け、既存fileを
// archive root直下の.preservedへ移します。予約名は元の分類を保持し、元file名の
// 長さに依存しません。移動先が存在した場合は別名を作り直します。
// rename成功後の同期に失敗しても、実際の移動結果を呼び出し側へ返します。
func preserveConflictingArchiveFile(
	ctx context.Context,
	rootPath string,
	failureCategory uploader.FailureCategory,
	path string,
	rename func(string, string) error,
) (fsstore.ConflictRename, error) {
	if !failureCategory.IsValid() {
		return fsstore.ConflictRename{}, fmt.Errorf("%w: %q", ErrInvalidFailureCategory, failureCategory)
	}
	preservedDirectory := filepath.Join(rootPath, fsstore.PreservedDirectoryName)
	if err := fsstore.EnsureDirectoryNoSymlinksContext(ctx, preservedDirectory, directoryMode); err != nil {
		return fsstore.ConflictRename{}, fmt.Errorf(
			"%w: prepare archive conflict preservation directory %q: %w",
			ErrCompressedLogAlreadyExists, preservedDirectory, err)
	}
	for range preservedConflictAttempts {
		if err := ctx.Err(); err != nil {
			return fsstore.ConflictRename{}, err
		}
		var token [16]byte
		if _, err := rand.Read(token[:]); err != nil {
			return fsstore.ConflictRename{}, fmt.Errorf("create archive conflict name: %w", err)
		}
		retainedPath := filepath.Join(preservedDirectory,
			preservedConflictPrefix+string(failureCategory)+"-"+hex.EncodeToString(token[:])+preservedConflictSuffix)
		if err := rename(path, retainedPath); err != nil {
			if errors.Is(err, fs.ErrExist) {
				continue
			}
			return fsstore.ConflictRename{}, fmt.Errorf(
				"%w: preserve conflicting archive file %q as %q: %w",
				ErrCompressedLogAlreadyExists, path, retainedPath, err)
		}
		conflict := fsstore.ConflictRename{OriginalPath: path, RetainedPath: retainedPath}
		var syncErrs []error
		for _, directory := range []string{filepath.Dir(path), preservedDirectory, rootPath} {
			if err := fsstore.SyncDirectory(directory); err != nil {
				syncErrs = append(syncErrs, fmt.Errorf("sync %q: %w", directory, err))
			}
		}
		if err := errors.Join(syncErrs...); err != nil {
			return conflict, fmt.Errorf(
				"%w: sync archive directories after preserving conflicting file %q as %q: %w",
				pkgfsstore.ErrDurabilityUnconfirmed, path, retainedPath, err)
		}
		return conflict, nil
	}
	return fsstore.ConflictRename{}, fmt.Errorf(
		"%w: reserve a name for conflicting archive file %q: all attempts collided",
		ErrCompressedLogAlreadyExists, path)
}
