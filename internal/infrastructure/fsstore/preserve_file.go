package fsstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// PreservedDirectoryNameは、自動削除・再送から外した保全ファイルの保存先です。
// 各ログ保存先・宛先のroot直下で、renameによる退避とコピーによる保全に共通で使います。
const PreservedDirectoryName = ".preserved"

// PreserveRegularFileはsourceをroot直下の.preservedへ内容を変更せずコピー保存します。
// basenameと内容のhashから名前を決め、同内容の再試行は既存fileを再利用します。
// 形式は解釈せずbyte単位で照合し、そのbytesからpayloadのhashを決めます。
// 保全fileは自動削除・再送せず、管理者が確認して手動対応します。
// 元fileと既存fileは削除・置換しません。呼び出し元はsourceの変更と保存操作を排他します。
// 公開後の同期に失敗した場合も保全pathを返し、次の呼び出しで同期を再試行します。
func PreserveRegularFile(ctx context.Context, rootPath, sourcePath string) (string, error) {
	return preserveRegularFile(ctx, rootPath, sourcePath, SyncDirectory)
}

func preserveRegularFile(ctx context.Context, rootPath, sourcePath string, syncDirectory func(string) error) (preservedPath string, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if strings.TrimSpace(rootPath) == "" || strings.TrimSpace(sourcePath) == "" {
		return "", errors.New("preserved file root and source must not be empty")
	}
	rootPath, err = filepath.Abs(rootPath)
	if err != nil {
		return "", fmt.Errorf("resolve preserved file root: %w", err)
	}
	source, sourceInfo, err := OpenRegularFile(sourcePath, os.O_RDONLY)
	if err != nil {
		return "", fmt.Errorf("open preserved file source: %w", err)
	}
	defer func() {
		if closeErr := source.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close preserved file source: %w", closeErr))
		}
	}()
	directory := filepath.Join(rootPath, PreservedDirectoryName)
	if err := EnsureDirectoryNoSymlinksContext(ctx, directory, 0o750); err != nil {
		return "", fmt.Errorf("prepare preserved file directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".preserve-*.tmp")
	if err != nil {
		return "", fmt.Errorf("create preserved file copy: %w", err)
	}
	temporaryPath := temporary.Name()
	temporaryOpen := true
	defer func() {
		if temporaryOpen {
			err = errors.Join(err, temporary.Close())
		}
		if removeErr := os.Remove(temporaryPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("remove unpublished preserved file copy: %w", removeErr))
		}
	}()
	if err := SetPrivateFilePermissions(temporary, 0o640); err != nil {
		return "", fmt.Errorf("set preserved file permissions: %w", err)
	}
	payloadHash := sha256.New()
	written, err := copyPreservedFileWithContext(ctx, io.MultiWriter(temporary, payloadHash), source)
	if err != nil {
		return "", fmt.Errorf("copy preserved file: %w", err)
	}
	afterInfo, err := source.Stat()
	if err != nil {
		return "", fmt.Errorf("inspect preserved file source after copy: %w", err)
	}
	if written != sourceInfo.Size() || afterInfo.Size() != sourceInfo.Size() || !afterInfo.ModTime().Equal(sourceInfo.ModTime()) {
		return "", errors.New("preserved file source changed while copying")
	}
	equal, err := EqualRegularFileContents(ctx, sourcePath, temporaryPath)
	if err != nil {
		return "", fmt.Errorf("verify preserved file copy: %w", err)
	}
	if !equal {
		return "", fmt.Errorf("preserved file %w", ErrCopyDiffersFromSource)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := temporary.Sync(); err != nil {
		return "", fmt.Errorf("%w: sync preserved file copy: %w", pkgfsstore.ErrDurabilityUnconfirmed, err)
	}
	closeErr := temporary.Close()
	temporaryOpen = false
	if closeErr != nil {
		return "", fmt.Errorf("close preserved file copy: %w", closeErr)
	}
	nameHash := sha256.Sum256([]byte(filepath.Base(sourcePath)))
	finalPath := filepath.Join(directory, hex.EncodeToString(nameHash[:])+"__"+hex.EncodeToString(payloadHash.Sum(nil))+".preserved")
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if renameErr := RenameFileNoReplace(temporaryPath, finalPath); renameErr != nil {
		if !errors.Is(renameErr, os.ErrExist) {
			return "", fmt.Errorf("place preserved file: %w", renameErr)
		}
		equal, err := EqualRegularFileContents(ctx, sourcePath, finalPath)
		if err != nil {
			return "", fmt.Errorf("verify existing preserved file: %w", err)
		}
		if !equal {
			return "", fmt.Errorf("existing preserved file %q differs from source", finalPath)
		}
		preservedPath = finalPath
		if err := syncExistingPreservedFile(ctx, finalPath); err != nil {
			return preservedPath, err
		}
	} else {
		preservedPath = finalPath
	}
	// root自身も毎回同期し、.preserved作成後の親同期が以前失敗した場合を回復します。
	for _, directory := range []string{directory, rootPath} {
		if err := syncDirectory(directory); err != nil {
			return preservedPath, fmt.Errorf("%w: sync preserved file directory %q: %w", pkgfsstore.ErrDurabilityUnconfirmed, directory, err)
		}
	}
	return preservedPath, ctx.Err()
}

func syncExistingPreservedFile(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// 同じ保全fileを並行して公開した処理のrename handleと共存して開きます。
	file, _, err := openRegularFile(path, os.O_RDWR, openFileSharingDelete)
	if err != nil {
		return fmt.Errorf("open existing preserved file for sync: %w", err)
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if syncErr != nil {
		return fmt.Errorf("%w: sync existing preserved file: %w", pkgfsstore.ErrDurabilityUnconfirmed, errors.Join(syncErr, closeErr))
	}
	if closeErr != nil {
		return fmt.Errorf("close existing preserved file: %w", closeErr)
	}
	return nil
}

func copyPreservedFileWithContext(ctx context.Context, destination io.Writer, source io.Reader) (int64, error) {
	buffer := make([]byte, 128*1024)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		read, readErr := source.Read(buffer)
		if read > 0 {
			if err := ctx.Err(); err != nil {
				return total, err
			}
			written, writeErr := destination.Write(buffer[:read])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != read {
				return total, io.ErrShortWrite
			}
		}
		if errors.Is(readErr, io.EOF) {
			return total, ctx.Err()
		}
		if readErr != nil {
			return total, readErr
		}
	}
}
