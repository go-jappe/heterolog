package fsstore

import (
	"errors"
	"fmt"
	"os"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// ErrCopyDiffersFromSourceは、保存・保全のために作ったコピーを読み戻した内容が元ファイルと
// 一致しないことを表します。各保存先は自身の文言でこのsentinelをwrapします。
var ErrCopyDiffersFromSource = errors.New("copy differs from source")

// ErrInvalidFileNameDateAndIndexは、ファイル名から日付・indexを解析できないことを表します。
var ErrInvalidFileNameDateAndIndex = errors.New("invalid file name date and index")

// FileNameDateAndIndexErrorは、日付・indexを解析できなかったファイル名のpathを示します。
// ErrInvalidFileNameDateAndIndexと、日付・indexの解析エラーがあればそれもwrapします。
type FileNameDateAndIndexError struct {
	Path string
	// Partは解析できなかった部分です。
	Part FileNameDateAndIndexPart
	// Errは日付・indexの解析エラーです。名前の形式が一致しない場合はnilです。
	Err error
}

// FileNameDateAndIndexPartは、ファイル名のうち解析できなかった部分です。
type FileNameDateAndIndexPart int

const (
	// FileNameDateAndIndexPatternは、名前が「接頭辞_YYYYMMDD_index.拡張子」に一致しないことを表します。
	FileNameDateAndIndexPattern FileNameDateAndIndexPart = iota + 1
	// FileNameDateAndIndexDateは、日付を暦日として解析できないことを表します。
	FileNameDateAndIndexDate
	// FileNameDateAndIndexIndexは、indexをintとして解析できないことを表します。
	FileNameDateAndIndexIndex
)

func (e *FileNameDateAndIndexError) Error() string {
	switch e.Part {
	case FileNameDateAndIndexDate:
		return fmt.Sprintf("invalid file name date in %q: %v", e.Path, e.Err)
	case FileNameDateAndIndexIndex:
		return fmt.Sprintf("invalid file name index in %q: %v", e.Path, e.Err)
	default:
		return fmt.Sprintf("file name does not contain a valid date and index: %q", e.Path)
	}
}

func (e *FileNameDateAndIndexError) Unwrap() []error {
	if e.Err == nil {
		return []error{ErrInvalidFileNameDateAndIndex}
	}
	return []error{ErrInvalidFileNameDateAndIndex, e.Err}
}

// NotRegularFileErrorは、pathが通常ファイルでないことを表します。ErrNotRegularFileをwrapするため、
// errors.Isで種類を、errors.Asで拒否したpathとmodeを判別できます。
type NotRegularFileError struct {
	Path string
	// Modeは検査したentryのmodeです。呼び出し箇所が保持していない場合は0です。
	Mode os.FileMode
	// messageは、呼び出し箇所ごとの従来の文言です。空なら既定の文言を使います。
	message string
}

func newNotRegularFileError(path string, mode os.FileMode, message string) *NotRegularFileError {
	return &NotRegularFileError{Path: path, Mode: mode, message: message}
}

func (e *NotRegularFileError) Error() string {
	if e.message != "" {
		return e.message
	}
	return fmt.Sprintf("%v: path=%q mode=%s", ErrNotRegularFile, e.Path, e.Mode)
}

func (e *NotRegularFileError) Unwrap() error {
	return ErrNotRegularFile
}

// FileRemovalStepは、ファイル削除のうち失敗した段階です。
type FileRemovalStep string

const (
	// FileRemovalStepInspectNameは、削除候補のファイル名を解析できなかった段階です。
	FileRemovalStepInspectName FileRemovalStep = "inspect file name for removal"
	// FileRemovalStepSortNamesは、削除候補をファイル名の順に並べ替えられなかった段階です。
	FileRemovalStepSortNames FileRemovalStep = "sort files by name for removal"
	// FileRemovalStepInspectは、削除前の情報取得に失敗した段階です。
	FileRemovalStepInspect FileRemovalStep = "stat file before removal"
	// FileRemovalStepRemoveは、ファイルの削除に失敗した段階です。
	FileRemovalStepRemove FileRemovalStep = "remove file"
	// FileRemovalStepSyncAfterRemoveは、削除後の親ディレクトリの同期に失敗した段階です。
	FileRemovalStepSyncAfterRemove FileRemovalStep = "sync parent directory after removing file"
	// FileRemovalStepSyncAfterAbsentは、不存在を確認した後の親ディレクトリの同期に失敗した段階です。
	FileRemovalStepSyncAfterAbsent FileRemovalStep = "sync parent directory after confirming file is absent"
)

// FileRemovalErrorは、ファイル削除の失敗した段階と対象のpathを示します。Errは原因です。
// 同期の段階ではErrもpkgfsstore.ErrDurabilityUnconfirmedをwrapします。
type FileRemovalError struct {
	Step FileRemovalStep
	Path string
	Err  error
}

func (e *FileRemovalError) Error() string {
	switch e.Step {
	case FileRemovalStepSyncAfterRemove:
		return fmt.Sprintf("sync parent directory after removing file %q: %v", e.Path, e.Err)
	case FileRemovalStepSyncAfterAbsent:
		return fmt.Sprintf("sync parent directory after confirming file %q is absent: %v", e.Path, e.Err)
	default:
		return fmt.Sprintf("%s: %v", e.Step, e.Err)
	}
}

func (e *FileRemovalError) Unwrap() error {
	return e.Err
}

// FileRemovalFailureは、JoinFileRemovalFailuresが返す一件分の失敗です。Fileは失敗した
// ファイルの観測情報、Errは削除の結果です。
type FileRemovalFailure struct {
	File FileInfo
	Err  error
}

func (e *FileRemovalFailure) Error() string {
	if e.File.ModTime.IsZero() {
		return fmt.Sprintf("file %s: %v", e.File.Path, e.Err)
	}
	return fmt.Sprintf("file %s (modTime=%s): %v", e.File.Path, e.File.ModTime.Format(time.RFC3339), e.Err)
}

func (e *FileRemovalFailure) Unwrap() error {
	return e.Err
}

// CompressionCandidateErrorは、圧縮元に対応する候補pathを検査できなかったことを示します。
type CompressionCandidateError struct {
	Path string
	Err  error
}

func (e *CompressionCandidateError) Error() string {
	return fmt.Sprintf("inspect compression candidate %q: %v", e.Path, e.Err)
}

func (e *CompressionCandidateError) Unwrap() error {
	return e.Err
}

// ErrCompressionFormatWithoutExtensionは、圧縮形式から有効な拡張子を作れず、
// 圧縮ファイルのpathを生成できないことを表します。
var ErrCompressionFormatWithoutExtension = errors.New("compression format has no valid file extension")

// CompressionFormatExtensionErrorは、圧縮ファイルのpathを生成できなかった圧縮形式を示します。
// ErrCompressionFormatWithoutExtensionをwrapします。
type CompressionFormatExtensionError struct {
	Format pkgfsstore.CompressFormat
}

func (e *CompressionFormatExtensionError) Error() string {
	return fmt.Sprintf("compression format %q has no valid file extension", e.Format)
}

func (e *CompressionFormatExtensionError) Unwrap() error {
	return ErrCompressionFormatWithoutExtension
}
