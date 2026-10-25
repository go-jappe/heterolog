package compressedlogfailure

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

var (
	// ErrNoDestinationServiceは、宛先のupload serviceを判別できないことを表します。
	// ErrInvalidConfigと共にwrapします。
	ErrNoDestinationService = errors.New("no service for destination")
	// ErrNoMatchingSpoolRuntimeは、宛先のupload serviceに対応するspool runtimeがないことを表します。
	ErrNoMatchingSpoolRuntime = errors.New("no matching spool runtime")
	// ErrUnexpectedSavedTargetは、退避先が自身の種類と異なる保存結果を返したことを表します。
	ErrUnexpectedSavedTarget = errors.New("unexpected saved failure target type")
	// ErrConflictingSavedFailureResultsは、退避先の保存結果が受理済みの記録と異なるため、
	// 元ファイルを保全したことを表します。
	ErrConflictingSavedFailureResults = errors.New("conflicting saved failure results")
	// ErrPreservedArchiveFilesは、archive rootの.preservedに管理者確認待ちのfileがあることを表します。
	ErrPreservedArchiveFiles = errors.New("preserved archive files require operator review")
	// ErrDestinationUploadPanicは、宛先への送信・退避処理のpanicを回復したことを表します。
	ErrDestinationUploadPanic = errors.New("upload destination panic")
	// ErrDurableStoreSavePanicは、退避先への保存のpanicを回復したことを表します。
	ErrDurableStoreSavePanic = errors.New("durable store save panic")
	// ErrArchivePurgePanicは、archiveの整理のpanicを回復したことを表します。
	ErrArchivePurgePanic = errors.New("compressed log file upload failure archive purge panic")
)

// SpoolRuntimeMismatchErrorは、spoolを選択した宛先のserviceに対応するspool runtimeがないことを示します。
// errors.IsでErrInvalidConfigとErrNoMatchingSpoolRuntimeの両方に一致します。
type SpoolRuntimeMismatchError struct {
	Destination objectstorage.DestinationID
	Service     objectstorage.UploadTarget
}

func (e *SpoolRuntimeMismatchError) Error() string {
	return fmt.Sprintf("%v: %v for destination %q service %q", ErrInvalidConfig, ErrNoMatchingSpoolRuntime, e.Destination, e.Service)
}

func (e *SpoolRuntimeMismatchError) Unwrap() []error {
	return []error{ErrInvalidConfig, ErrNoMatchingSpoolRuntime}
}

// InvalidDestinationIDErrorは、宛先IDを失敗時の保存先の名前に使えないことを示す構築時の設定エラーです。
// 宛先IDは宛先別spoolとfile archiveのdirectory、Retry-Afterのmarker（.retry-after/<宛先ID>.json）の
// 名前になるため、retryafter.ValidateDestinationIDと同じ規則で検証します。
// errors.IsでErrInvalidConfigと、規則に反した理由のretryafter.ErrInvalidDestinationIDの両方に一致します。
type InvalidDestinationIDError struct {
	Destination objectstorage.DestinationID
	// Errは規則に反した理由です。
	Err error
}

func (e *InvalidDestinationIDError) Error() string {
	return fmt.Sprintf("%v: destination %q cannot name the failure store directories: %v", ErrInvalidConfig, e.Destination, e.Err)
}

func (e *InvalidDestinationIDError) Unwrap() []error {
	return []error{ErrInvalidConfig, e.Err}
}

// SavedTargetTypeErrorは、Kindの退避先が返した保存結果の型がWantではないことを示します。
type SavedTargetTypeError struct {
	Kind failurestore.DurableType
	// Nameは退避先の表示名です。
	Name string
	// Savedは退避先が返した保存結果です。
	Saved checkpoint.SavedFailureTarget
	// Wantは期待した保存結果の型名です。
	Want string
}

func (e *SavedTargetTypeError) Error() string {
	return fmt.Sprintf("%s returned saved target %T, want %s", e.Name, e.Saved, e.Want)
}

func (e *SavedTargetTypeError) Unwrap() error {
	return ErrUnexpectedSavedTarget
}

// ConflictPreservationWarningは、保存結果の矛盾に備えて元ファイルを保全したことを示す警告です。
// Notification.PreservationWarningは、保全した矛盾ごとのこの警告をまとめます。
type ConflictPreservationWarning struct {
	SourcePath  string
	Destination objectstorage.DestinationID
	// Targetは矛盾した退避先のrootです。
	Target   string
	Recorded checkpoint.SavedFailureTarget
	Received checkpoint.SavedFailureTarget
	// PreservedPathは、purge対象外の保全fileのpathです。
	PreservedPath string
}

func (w *ConflictPreservationWarning) Error() string {
	return fmt.Sprintf(
		"conflicting saved failure results for %q (destination %q, target %q): recorded=%+v received=%+v; source copied to purge-excluded preservation file %q; administrator review required",
		w.SourcePath, w.Destination, w.Target, w.Recorded, w.Received, w.PreservedPath,
	)
}

func (w *ConflictPreservationWarning) Unwrap() error {
	return ErrConflictingSavedFailureResults
}

// ConflictPreservationErrorは、保存結果の矛盾に備えた元ファイルの保全に失敗したことを示します。
// 保全できるまで矛盾は解消せず、完了cursorも進めません。
type ConflictPreservationError struct {
	SourcePath string
	// TargetNameは保全先の退避先の表示名です。
	TargetName string
	Err        error
}

func (e *ConflictPreservationError) Error() string {
	return fmt.Sprintf("preserve source %q after conflicting %s results: %v", e.SourcePath, e.TargetName, e.Err)
}

func (e *ConflictPreservationError) Unwrap() error {
	return e.Err
}

// PreservedArchiveFilesErrorは、archive rootに管理者確認待ちの保全fileが残っていることを示します。
type PreservedArchiveFilesError struct {
	Root  string
	Count int
}

func (e *PreservedArchiveFilesError) Error() string {
	return fmt.Sprintf(
		"compressed log file upload failure archive %q has %d preserved files; operator review required",
		e.Root, e.Count,
	)
}

func (e *PreservedArchiveFilesError) Unwrap() error {
	return ErrPreservedArchiveFiles
}

// PanicErrorは、回復したpanicを示します。KindはErrDestinationUploadPanic、
// ErrDurableStoreSavePanic、ErrArchivePurgePanicのいずれかで、errors.Isで判定できます。
type PanicError struct {
	Kind error
	// Destinationは、ErrDestinationUploadPanicの場合の宛先です。
	Destination objectstorage.DestinationID
	// Valueは回復したpanicの値です。
	Value any
}

func (e *PanicError) Error() string {
	if errors.Is(e.Kind, ErrDestinationUploadPanic) {
		return fmt.Sprintf("upload destination %q panic: %v", e.Destination, e.Value)
	}
	return fmt.Sprintf("%v: %v", e.Kind, e.Value)
}

func (e *PanicError) Unwrap() error {
	return e.Kind
}
