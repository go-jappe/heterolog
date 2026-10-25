package spool

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// DirectoryPreservationは、spoolのディレクトリ作成を妨げたエントリの隔離結果です。
// RetainedPathはroot直下の.preserved内です。再送・purgeはここを処理しません。
// 呼び出し元は成功時もOnWarnへ元パス・隔離先・Reasonを通知してください。
type DirectoryPreservation struct {
	OriginalPath string
	RetainedPath string
	Reason       error
}

// EnsureDirectoryWithPreservationはstateとその配下の日時別保存先を用意します。不正なファイルや
// リンクが作成を妨げていれば、そのエントリ自身を.preservedへ上書きせず移し、
// ディレクトリを作り直します。リンク先や正常なディレクトリには触れません。
// 同じrootの検査・隔離・再作成はStore instanceをまたいで排他します。
// 作成するdirectoryの権限と既存directoryの権限の扱いはEnsureDirectoryNoSymlinksと同じです。
// rootは初期化時に解決した実体パスを渡してください。rootやその祖先の変更、
// 権限・I/Oエラー、保全先の不正は自動復旧せず返します。別プロセスの同時変更は対象外です。
// 移動後の同期に失敗した場合も、完了した移動を戻り値に含めてエラーを返します。
func EnsureDirectoryWithPreservation(ctx context.Context, rootDirectory, directory string) ([]DirectoryPreservation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	release, err := lockDirectoryPreparation(ctx, rootDirectory)
	if err != nil {
		return nil, err
	}
	defer release()
	root, err := filepath.Abs(rootDirectory)
	if err != nil {
		return nil, err
	}
	resolved, err := pathidentity.ResolveDirectory(root)
	if err != nil {
		return nil, &RootError{Root: root, Err: err}
	}
	if pathidentity.ComparisonKey(root) != pathidentity.ComparisonKey(resolved) {
		return nil, &RootError{Root: root, Link: true}
	}
	relative, err := filepath.Rel(root, directory)
	if err != nil || !filepath.IsLocal(relative) || relative == "." {
		return nil, &DirectoryRangeError{Directory: directory, Root: root, OutsideRoot: true}
	}
	if state := pkgspool.State(strings.Split(relative, string(filepath.Separator))[0]); !state.IsValid() {
		return nil, &DirectoryRangeError{Directory: directory, Root: root}
	}
	var preserved []DirectoryPreservation
	for {
		if err := ctx.Err(); err != nil {
			return preserved, err
		}
		err := ensureDirectoryNoSymlinksLocked(root, directory)
		if err == nil {
			return preserved, nil
		}
		var obstruction *pathidentity.ManagedDirectoryObstructionError
		if !errors.As(err, &obstruction) {
			return preserved, err
		}
		result, preserveErr := preserveDirectoryObstructionLocked(ctx, root, obstruction)
		if result.RetainedPath != "" {
			preserved = append(preserved, result)
		}
		if preserveErr != nil {
			return preserved, preserveErr
		}
	}
}

func preserveDirectoryObstructionLocked(ctx context.Context, root string, obstruction *pathidentity.ManagedDirectoryObstructionError) (DirectoryPreservation, error) {
	path := obstruction.Path
	relative, err := filepath.Rel(root, path)
	if err != nil || !filepath.IsLocal(relative) || relative == "." {
		return DirectoryPreservation{}, fmt.Errorf("cannot preserve obstruction outside spool root: %q", path)
	}
	parent := filepath.Dir(path)
	if parent != root {
		parentRelative, err := filepath.Rel(root, parent)
		if err != nil {
			return DirectoryPreservation{}, err
		}
		if _, err := pathidentity.ResolveAndValidateManagedDirectory(root, parentRelative); err != nil {
			return DirectoryPreservation{}, err
		}
	}
	info, err := os.Lstat(path)
	if err != nil {
		return DirectoryPreservation{}, err
	}
	if !os.SameFile(info, obstruction.Info) {
		return DirectoryPreservation{}, fmt.Errorf("spool directory obstruction %q changed before preservation", path)
	}
	preservedDirectory := filepath.Join(root, infrafsstore.PreservedDirectoryName)
	if err := ensureDirectoryNoSymlinksLocked(root, preservedDirectory); err != nil {
		return DirectoryPreservation{}, fmt.Errorf("prepare spool preserved directory: %w", err)
	}
	// 新しく作った.preserved自体の名前も、エントリを移す前に同期します。
	if err := infrafsstore.SyncDirectory(root); err != nil {
		return DirectoryPreservation{}, fmt.Errorf("%w: sync spool preserved directory parent: %w", pkgfsstore.ErrDurabilityUnconfirmed, err)
	}
	digest := sha256.Sum256([]byte(relative))
	for range 8 {
		if err := ctx.Err(); err != nil {
			return DirectoryPreservation{}, err
		}
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return DirectoryPreservation{}, err
		}
		name := filepath.Base(path) + "__" + hex.EncodeToString(digest[:8]) + "__" + hex.EncodeToString(nonce[:]) + ".directory-obstruction"
		retained := filepath.Join(preservedDirectory, name)
		if err := infrafsstore.RenameFileNoReplace(path, retained); err != nil {
			if errors.Is(err, os.ErrExist) {
				continue
			}
			return DirectoryPreservation{}, fmt.Errorf("preserve spool directory obstruction %q: %w", path, err)
		}
		result := DirectoryPreservation{OriginalPath: path, RetainedPath: retained, Reason: obstruction}
		// rename後は中止要求があっても両ディレクトリの同期を試し、移動先を返します。
		syncErr := errors.Join(infrafsstore.SyncDirectory(preservedDirectory), infrafsstore.SyncDirectory(parent))
		if syncErr != nil {
			return result, fmt.Errorf("%w: sync preserved spool directory obstruction: %w", pkgfsstore.ErrDurabilityUnconfirmed, syncErr)
		}
		return result, nil
	}
	return DirectoryPreservation{}, fmt.Errorf("preserve spool directory obstruction %q: unique names exhausted", path)
}
