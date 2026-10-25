package appendlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
)

var ErrCompressionSourceDeletePendingConflict = errors.New(
	"appendlog compression source and its delete-pending path both exist",
)

// CompressionSourceDeletePendingConflictErrorは、圧縮元ログと削除待ちpathが同時に存在し、
// どちらかを安全に削除できないことを表します。errors.Isでは
// ErrCompressionSourceDeletePendingConflictと一致し、errors.Asで各pathを取り出せます。
// ContentsDifferは両方の内容を比較して異なると判定したことを表し、そのときPreservedPathに
// 削除待ちfileの退避先を設定します。退避に失敗した場合のPreservedPathは空です。
type CompressionSourceDeletePendingConflictError struct {
	SourcePath     string
	PendingPath    string
	PreservedPath  string
	ContentsDiffer bool
}

func (e *CompressionSourceDeletePendingConflictError) Error() string {
	if e.ContentsDiffer {
		return fmt.Sprintf(
			"%s: contents differ: source=%q pending=%q preserved=%q",
			ErrCompressionSourceDeletePendingConflict,
			e.SourcePath,
			e.PendingPath,
			e.PreservedPath,
		)
	}
	return fmt.Sprintf(
		"%s: source=%q pending=%q",
		ErrCompressionSourceDeletePendingConflict,
		e.SourcePath,
		e.PendingPath,
	)
}

func (e *CompressionSourceDeletePendingConflictError) Is(target error) bool {
	return target == ErrCompressionSourceDeletePendingConflict
}

const compressionSourceConflictNamePattern = ".compression-source-conflict-*.preserved"

// ErrNotCompressionSourceDeletePendingPathは、削除しようとしたpathが元ログの削除待ちfileの
// 名前ではないことを表します。誤削除を防ぐため、このpathは削除しません。
var ErrNotCompressionSourceDeletePendingPath = errors.New("path is not an appendlog compression source delete-pending file")

// ErrUnresolvableCompressionSourcePathは、削除待ちfileの名前から元ログのpathを復元できないことを表します。
var ErrUnresolvableCompressionSourcePath = errors.New("resolve appendlog compression source from delete-pending path")

// CompressionSourceRetirementErrorは、元ログの削除待ちfileを片付けられず、元ログの
// retirementを完了できなかったことを示します。CompressedPathは圧縮後の削除の場合だけ設定し、
// full scanで見つけた削除待ちfileの回復では空です。
type CompressionSourceRetirementError struct {
	SourcePath     string
	PendingPath    string
	CompressedPath string
	Err            error
}

func (e *CompressionSourceRetirementError) Error() string {
	message := fmt.Sprintf(
		"finish compression source retirement: source=%q pending=%q: %v",
		e.SourcePath, e.PendingPath, e.Err,
	)
	if e.CompressedPath == "" {
		return message
	}
	return fmt.Sprintf("retire source for compressed file %q: %s", e.CompressedPath, message)
}

func (e *CompressionSourceRetirementError) Unwrap() error {
	return e.Err
}

// ErrCompressionSourceCleanupは、圧縮後fileの公開とcanonical sourceのretirement後に、
// pending sourceの削除、または削除結果のdirectory同期を完了できなかったことを示します。
// errors.Isで判定し、圧縮済みfileのarchive/uploadは継続できます。
var ErrCompressionSourceCleanup = errors.New("compression source cleanup failed")

// RemoveSourceAfterCompressionは、圧縮が完了した元ログを削除します。圧縮ファイルは削除しません。
// 元ログの名前に.compression-source-delete-pending.tmpを付け、親ディレクトリを同期してから
// 削除します。削除後も親ディレクトリを同期します。名前変更を先に確定することで、削除に
// 失敗しても元ログを再圧縮の対象に戻さず、次回は削除待ちファイルとして片付けられます。
//
// 呼び出し元はパスの入力検証と圧縮ファイルの作成・検証を済ませ、同じディレクトリの
// lifecycle lockを保持してください。この関数は圧縮ファイルの作成・検証を行いません。
// 別プロセスやlifecycle外の手動操作との同時更新は対象外です。
func RemoveSourceAfterCompression(sourcePath, compressedPath string) error {
	return removeSourceAfterCompression(
		sourcePath,
		compressedPath,
		os.Rename,
		os.Remove,
		infrafsstore.SyncDirectory,
	)
}

// removeCompressionSourceDeletePendingPathは、元ログの削除待ちファイルを、
// 親ディレクトリの同期後に削除し、削除結果も同期します。既に削除されていても同期は行います。
// ファイル名が削除待ち用の形式でない場合は、誤削除を防ぐため拒否します。
// preRemovalSyncCompletedは最初の同期が成功したことだけを表します。元ログの削除完了や
// 圧縮の成功は意味しません。ErrCompressionSourceCleanupを付ける判断は呼び出し側が行います。
// 関数引数には通常os.Removeとfsstore.SyncDirectoryを渡し、試験では操作の失敗を再現します。
func removeCompressionSourceDeletePendingPath(
	path string,
	removePendingSource removeCompressionSourceFunc,
	syncDirectory syncCompressionDirectoryFunc,
) (preRemovalSyncCompleted bool, err error) {
	if !logpattern.IsCompressionSourceDeletePendingPath(path) {
		return false, fmt.Errorf("%w: %q", ErrNotCompressionSourceDeletePendingPath, path)
	}
	if syncDirectory == nil {
		return false, errors.New("appendlog compression directory sync function is nil")
	}
	// pending pathが最初のretirement directory sync失敗後に残った可能性があります。
	// 削除より先にrename結果を同期し、canonical sourceへ戻り得る状態を解消します。
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return false, fmt.Errorf(
			"sync directory before removing appendlog compression source delete-pending file %q: %w",
			path,
			err,
		)
	}
	if err := removePendingSource(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return true, fmt.Errorf("remove appendlog compression source delete-pending file %q: %w", path, err)
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return true, fmt.Errorf(
			"sync directory after removing appendlog compression source delete-pending file %q: %w",
			path,
			err,
		)
	}
	return true, nil
}

// RecoverCompressionSourceDeletePendingPathはfull scanで見つけたpending pathを、同じ
// generationのcanonical sourceの有無に応じて回復します。
//
// canonical sourceも存在する場合、両fileの内容が同じと確認できた場合だけpendingを
// 削除します。異なるpendingはログdirectory内の.preservedへ退避し、退避先を含む
// errorを返してcallerのOnErrorへ通知します。比較に失敗した場合は両方をそのまま残します。
// いずれもsourceは通常回復へ残すため戻り値はfalseです。
// sourceが存在しない場合は、raw→pending renameのdirectory同期を先に確定してから
// pendingを削除します。この場合、削除または
// 削除後同期だけが失敗してもsource retirement自体はdurableなのでtrueを返します。
// 圧縮ファイルの探索・生成は行いません。圧縮済みfileが外部削除されていても
// pendingの後片付けは実行でき、戻り値のtrueはupload可能なfileの存在を保証しません。
// sourcePathはpending名の復元時に検証します。
func RecoverCompressionSourceDeletePendingPath(
	pendingPath string,
) (sourceRetirementDurable bool, err error) {
	sourcePath, ok := logpattern.CompressionSourcePathFromDeletePendingPath(pendingPath)
	if !ok {
		return false, fmt.Errorf("%w %q", ErrUnresolvableCompressionSourcePath, pendingPath)
	}

	// 圧縮元ログが通常名でも残っているか確認し、削除待ちpathとの競合を判定します。
	sourceExists, err := infrafsstore.PathExists(sourcePath)
	if err != nil {
		return false, fmt.Errorf("stat appendlog compression source %q: %w", sourcePath, err)
	}
	if sourceExists {
		// sourceとpendingの共存時だけ内容を比較します。サイズ一致だけでは、
		// pendingにしか残っていないログを削除してよいとは判断できません。
		equal, err := infrafsstore.EqualRegularFileContents(context.Background(), sourcePath, pendingPath)
		if err != nil {
			return false, fmt.Errorf(
				"compare appendlog compression source and delete-pending file: source=%q pending=%q: %w",
				sourcePath,
				pendingPath,
				err,
			)
		}
		if equal {
			_, err := removeCompressionSourceDeletePendingPath(pendingPath, os.Remove, infrafsstore.SyncDirectory)
			// 同期が完了しても元ログは通常名で残るため、圧縮後の削除が確定したとは扱いません。
			return false, err
		}
		preservedPath, preservationErr := preserveCompressionSourceDeletePendingPath(sourcePath, pendingPath)
		// 退避に成功しても内容の競合は運用者へ通知します。退避失敗も同じerrorへ
		// 含め、元のpendingか退避先のどちらにデータが残るかを明示します。
		conflictErr := &CompressionSourceDeletePendingConflictError{
			SourcePath:     sourcePath,
			PendingPath:    pendingPath,
			PreservedPath:  preservedPath,
			ContentsDiffer: true,
		}
		return false, errors.Join(conflictErr, preservationErr)
	}

	err = finishCompressionSourceRetirement(
		sourcePath, pendingPath, "", os.Remove, infrafsstore.SyncDirectory,
	)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, ErrCompressionSourceCleanup) {
		return true, err
	}
	return false, err
}

// preserveCompressionSourceDeletePendingPathは内容が異なるpendingを.preservedへ移します。
// CreateTempで専用の空fileを排他的に確保してから置き換え、過去の保全fileは上書きしません。
// 保全先は通常の圧縮・upload・purge・pending回復対象外で、管理者の確認へ残します。
// 戻り値のpathはrename成功後だけ設定し、その後のdirectory同期失敗時にも返します。
func preserveCompressionSourceDeletePendingPath(sourcePath, pendingPath string) (string, error) {
	directory := filepath.Join(filepath.Dir(sourcePath), infrafsstore.PreservedDirectoryName)
	if err := infrafsstore.EnsureDirectoryNoSymlinks(directory, 0o750); err != nil {
		return "", fmt.Errorf("prepare compression source preservation directory: %w", err)
	}
	reserved, err := os.CreateTemp(
		directory,
		filepath.Base(sourcePath)+compressionSourceConflictNamePattern,
	)
	if err != nil {
		return "", fmt.Errorf("reserve compression source preservation path: %w", err)
	}
	preservedPath := reserved.Name()
	renamed := false
	defer func() {
		if !renamed {
			// この呼出しが確保した空fileだけを片付け、pendingは削除しません。
			_ = os.Remove(preservedPath)
		}
	}()
	if err := reserved.Close(); err != nil {
		return "", fmt.Errorf("close reserved compression source preservation file: %w", err)
	}
	if err := os.Rename(pendingPath, preservedPath); err != nil {
		return "", fmt.Errorf("preserve compression source delete-pending file: %w", err)
	}
	renamed = true
	var errs []error
	for _, changedDirectory := range []string{directory, filepath.Dir(pendingPath)} {
		if err := infrafsstore.SyncDirectory(changedDirectory); err != nil {
			errs = append(errs, fmt.Errorf("sync compression source preservation directory %q: %w", changedDirectory, err))
		}
	}
	return preservedPath, errors.Join(errs...)
}

type renameCompressionSourceFunc func(oldPath, newPath string) error
type removeCompressionSourceFunc func(path string) error

// syncCompressionDirectoryFuncは、同期対象のdirectory pathを受け取ります。
type syncCompressionDirectoryFunc func(directoryPath string) error

// removeSourceAfterCompressionは、圧縮済みの元ログを削除待ちの名前へ変更し、
// 親ディレクトリを同期してから削除します。削除後も親ディレクトリを同期します。
// 名前変更を先に確定し、削除に失敗しても元ログが再圧縮されることを防ぎます。
// 圧縮ファイルの作成・検証とパスの入力検証は、呼び出し元で完了している前提です。
// 末尾の3つの関数引数は、試験で名前変更・削除・同期の失敗を再現するために渡します。
func removeSourceAfterCompression(
	sourcePath string,
	compressedPath string,
	renameSource renameCompressionSourceFunc,
	removePendingSource removeCompressionSourceFunc,
	syncDirectory syncCompressionDirectoryFunc,
) error {
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	// 圧縮後に退避・削除する元ログのpathが残っているか確認します。
	sourceExists, err := infrafsstore.PathExists(sourcePath)
	if err != nil {
		return fmt.Errorf("stat appendlog compression source %q: %w", sourcePath, err)
	}
	// 圧縮元ログを退避する削除待ちpathが既に存在するか確認します。
	pendingExists, err := infrafsstore.PathExists(pendingPath)
	if err != nil {
		return fmt.Errorf("stat appendlog compression source delete-pending path %q: %w", pendingPath, err)
	}

	if sourceExists && pendingExists {
		return &CompressionSourceDeletePendingConflictError{SourcePath: sourcePath, PendingPath: pendingPath}
	}
	if sourceExists {
		if err := renameSource(sourcePath, pendingPath); err != nil {
			return fmt.Errorf(
				"move compressed appendlog source to delete-pending path: source=%q pending=%q: %w",
				sourcePath,
				pendingPath,
				err,
			)
		}
	}

	return finishCompressionSourceRetirement(sourcePath, pendingPath, compressedPath, removePendingSource, syncDirectory)
}

// finishCompressionSourceRetirementは、通常名の元ログが既にない状態で削除待ちを片付けます。
// 同期・削除は共通関数へ委ね、最初の同期後の失敗だけErrCompressionSourceCleanupに分類します。
// この時点では元ログの名前変更が確定しており、残る失敗は削除待ちの後片付けだけです。
// compressedPathは圧縮後の削除の場合だけ渡し、失敗の*CompressionSourceRetirementErrorに記録します。
func finishCompressionSourceRetirement(
	sourcePath, pendingPath, compressedPath string,
	removePendingSource removeCompressionSourceFunc,
	syncDirectory syncCompressionDirectoryFunc,
) error {
	preRemovalSyncCompleted, err := removeCompressionSourceDeletePendingPath(pendingPath, removePendingSource, syncDirectory)
	if err == nil {
		return nil
	}
	if preRemovalSyncCompleted {
		err = fmt.Errorf("%w: %w", ErrCompressionSourceCleanup, err)
	}
	return &CompressionSourceRetirementError{
		SourcePath: sourcePath, PendingPath: pendingPath, CompressedPath: compressedPath, Err: err,
	}
}
