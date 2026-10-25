package uploader

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// DestinationUploadErrorは、一つの宛先へのuploadが失敗したことを表します。
// UploadReport.Errは失敗した宛先ごとにこのerrorを返し、errors.Joinでまとめます。
// errors.Asで失敗した宛先を取り出せ、原因はerrors.Isやerrors.Asで判別できます。
type DestinationUploadError struct {
	// Destinationは失敗した宛先のIDです。
	Destination objectstorage.DestinationID
	// Errはupload失敗の原因です。
	Err error
}

// Errorは、宛先IDと原因を含む説明を返します。
func (e *DestinationUploadError) Error() string {
	return fmt.Sprintf("object storage destination %q failed: %v", e.Destination, e.Err)
}

// Unwrapは、upload失敗の原因を返します。
func (e *DestinationUploadError) Unwrap() error { return e.Err }

var (
	// ErrUnrequestedDestinationResultは、要求していない宛先の結果が含まれることを表します。
	ErrUnrequestedDestinationResult = errors.New("upload report contains an unrequested destination")
	// ErrDuplicateDestinationResultは、同じ宛先の結果が複数あることを表します。
	ErrDuplicateDestinationResult = errors.New("upload report classifies a destination more than once")
	// ErrMissingDestinationResultは、要求した宛先の結果がないことを表します。
	ErrMissingDestinationResult = errors.New("upload report does not classify a requested destination")
)

// UploadReportErrorは、upload結果の一件が要求した宛先と一対一に対応しないことを表します。
// errors.Isでは、ErrInvalidUploadReportと、不正の種類を表すErrの両方に一致します。
type UploadReportError struct {
	// Destinationは、対応しない結果または欠けた結果の宛先IDです。
	Destination objectstorage.DestinationID
	// Indexは、ErrUnrequestedDestinationResultの場合に、UploadReport.Results内の位置です。
	// それ以外では0です。
	Index int
	// Errは、ErrUnrequestedDestinationResult、ErrDuplicateDestinationResult、
	// ErrMissingDestinationResultのいずれかです。
	Err error
}

// Errorは、不正の種類に応じて、宛先IDと結果の位置を含む説明を返します。
func (e *UploadReportError) Error() string {
	switch e.Err {
	case ErrUnrequestedDestinationResult:
		return fmt.Sprintf(
			"%v: result[%d] contains unrequested destination %q",
			ErrInvalidUploadReport, e.Index, e.Destination,
		)
	case ErrDuplicateDestinationResult:
		return fmt.Sprintf("%v: destination %q is classified more than once", ErrInvalidUploadReport, e.Destination)
	case ErrMissingDestinationResult:
		return fmt.Sprintf("%v: destination %q is not classified", ErrInvalidUploadReport, e.Destination)
	default:
		return fmt.Sprintf("%v: destination %q: %v", ErrInvalidUploadReport, e.Destination, e.Err)
	}
}

// Unwrapは、ErrInvalidUploadReportと不正の種類を表すErrを返します。
func (e *UploadReportError) Unwrap() []error { return []error{ErrInvalidUploadReport, e.Err} }
