package objectstoragespool

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// adoptOrPreserveExistingFilesは同名の既存ファイルを全stateから調べます。
// 同内容の一件は再利用し、それ以外は再送・purge対象外の.preservedへ移します。
// processingにある別内容のファイルは再送中かもしれないため動かしません。
// 呼び出し元はoperationMuを保持してください。
func (s *Store) adoptOrPreserveExistingFiles(
	ctx context.Context,
	sourcePath, fileName string,
	conflictRenames *[]infrafsstore.ConflictRename,
) (CompressedLogFile, bool, error) {
	files, err := s.findPublishedFiles(ctx, fileName, conflictRenames)
	if err != nil || len(files) == 0 {
		return CompressedLogFile{}, false, err
	}
	var keep *CompressedLogFile
	for index := range files {
		file := &files[index]
		equal, err := filesHaveEqualContent(ctx, sourcePath, file.Path)
		if err != nil {
			return CompressedLogFile{}, false, fmt.Errorf("compare existing compressed log file %q: %w", file.Path, err)
		}
		if !equal {
			if file.State == pkgspool.StateProcessing {
				return CompressedLogFile{}, false, fmt.Errorf(
					"%w: processing file %q differs from source and may be in use; retain it for the worker or administrator",
					ErrCompressedLogAlreadyExists, file.Path,
				)
			}
			continue
		}
		// 再送中の同内容fileがあれば、その処理を妨げずに再利用します。
		if keep == nil || file.State == pkgspool.StateProcessing {
			keep = file
		}
	}
	for _, file := range files {
		if keep != nil && file.Path == keep.Path {
			continue
		}
		if file.State == pkgspool.StateProcessing {
			return CompressedLogFile{}, false, fmt.Errorf(
				"%w: cannot move in-use processing file %q", ErrCompressedLogAlreadyExists, file.Path,
			)
		}
		if err := s.preserveConflictingFile(ctx, file, conflictRenames); err != nil {
			return CompressedLogFile{}, false, err
		}
	}
	if keep == nil {
		return CompressedLogFile{}, false, nil
	}
	// 上の照合はoperationMuの内側で済んでいるため、再度全内容を読み直しません。
	if err := s.refreshReusedFileModificationTime(ctx, *keep); err != nil {
		return CompressedLogFile{}, false, err
	}
	return *keep, true, nil
}

// preserveConflictingFileは既存fileを名前が衝突しない保管領域へ移します。
// renameが成功した時点で通知対象へ追加し、その後のdirectory同期失敗も返します。
func (s *Store) preserveConflictingFile(
	ctx context.Context,
	file CompressedLogFile,
	conflictRenames *[]infrafsstore.ConflictRename,
) error {
	stateDir, _ := s.root.StateDir(file.State)
	if err := ensureDirectChildRegularFile(stateDir, file.Path); err != nil {
		return fmt.Errorf("inspect conflicting compressed log file %q: %w", file.Path, err)
	}
	rename, err := s.preserveStateEntryLocked(ctx, file.State, file.FileName)
	if rename.RetainedPath != "" {
		*conflictRenames = append(*conflictRenames, rename)
	}
	return err
}

// preserveStateEntryLockedは、管理state直下のエントリを.preservedへ上書きせず
// 移します。リンク先はたどらず、ディレクトリは中身ごと移します。
// 呼び出し元はoperationMuを保持してください。移動後の同期が失敗しても移動先を返します。
func (s *Store) preserveStateEntryLocked(ctx context.Context, state pkgspool.State, fileName string) (infrafsstore.ConflictRename, error) {
	if _, err := s.root.StateDir(state); err != nil {
		return infrafsstore.ConflictRename{}, err
	}
	if fileName == "" || fileName == "." || fileName == ".." || filepath.Base(fileName) != fileName {
		return infrafsstore.ConflictRename{}, fmt.Errorf("%w: preservation requires one file name", ErrInvalidCompressedLogPath)
	}
	stateDir, err := pathidentity.ResolveAndValidateManagedDirectory(s.root.Path(), string(state))
	if err != nil {
		return infrafsstore.ConflictRename{}, fmt.Errorf("inspect preservation source directory: %w", err)
	}
	if _, err := pathidentity.ResolveAndValidateManagedDirectory(s.root.Path(), infrafsstore.PreservedDirectoryName); err != nil {
		return infrafsstore.ConflictRename{}, fmt.Errorf("inspect preserved directory: %w", err)
	}
	sourcePath := filepath.Join(stateDir, fileName)
	if _, err := os.Lstat(sourcePath); err != nil {
		return infrafsstore.ConflictRename{}, fmt.Errorf("inspect preservation source %q: %w", sourcePath, err)
	}
	if err := contextError(ctx); err != nil {
		return infrafsstore.ConflictRename{}, err
	}
	var retainedPath string
	for range 8 {
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return infrafsstore.ConflictRename{}, fmt.Errorf("name retained compressed log file: %w", err)
		}
		retainedPath = filepath.Join(s.root.preservedDir(), retainedConflictName(state, fileName, nonce))
		if err := infrafsstore.RenameFileNoReplace(sourcePath, retainedPath); err != nil {
			if errors.Is(err, os.ErrExist) {
				continue
			}
			return infrafsstore.ConflictRename{}, fmt.Errorf("retain conflicting compressed log file %q: %w", sourcePath, err)
		}
		rename := infrafsstore.ConflictRename{
			OriginalPath: sourcePath,
			RetainedPath: retainedPath,
		}
		if err := infrafsstore.SyncDirectory(s.root.preservedDir()); err != nil {
			return rename, fmt.Errorf("%w: sync retained compressed log file %q: %w", pkgfsstore.ErrDurabilityUnconfirmed, retainedPath, err)
		}
		if err := infrafsstore.SyncDirectory(stateDir); err != nil {
			return rename, fmt.Errorf("%w: sync removal of conflicting compressed log file name %q: %w", pkgfsstore.ErrDurabilityUnconfirmed, sourcePath, err)
		}
		return rename, nil
	}
	return infrafsstore.ConflictRename{}, fmt.Errorf("retain conflicting compressed log file %q: all unique names already exist", sourcePath)
}

// retainedConflictNameは長い元basenameでも一つのpath要素が長くなり過ぎないよう、
// 読みやすい先頭64文字、元basenameのhash、無作為な識別子を組み合わせます。
func retainedConflictName(state pkgspool.State, fileName string, nonce [16]byte) string {
	prefix := make([]byte, 0, 64)
	for i := 0; i < len(fileName) && len(prefix) < 64; i++ {
		character := fileName[i]
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '.' || character == '_' || character == '-' {
			prefix = append(prefix, character)
		} else {
			prefix = append(prefix, '-')
		}
	}
	nameHash := sha256.Sum256([]byte(fileName))
	return string(state) + "__" + string(prefix) + "__" + hex.EncodeToString(nameHash[:]) +
		"__" + hex.EncodeToString(nonce[:]) + ".conflict"
}
