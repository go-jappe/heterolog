package validation

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

var (
	// ErrUnsupportedUploadTargetは、upload targetに対応するproviderがないことを表します。
	// UnsupportedUploadTargetErrorは、errors.Isでこのsentinelと一致します。
	ErrUnsupportedUploadTarget = errors.New("unsupported object storage upload target")
	// ErrUploadTargetRequiredは、upload targetが空または空白だけであることを表します。
	ErrUploadTargetRequired = errors.New("object storage upload target is required")
	// ErrNonCanonicalUploadTargetは、upload targetが前後の空白を除いた小文字の表記では
	// ないことを表します。
	ErrNonCanonicalUploadTarget = errors.New("object storage upload target must use canonical form")
	// ErrDuplicateUploadTargetは、同じupload targetが複数回指定されたことを表します。
	ErrDuplicateUploadTarget = errors.New("duplicate object storage upload target")
	// ErrUploadTargetsRequiredは、有効なupload先にupload targetが一つも指定されていないことを
	// 表します。一覧内の空のtargetは、ErrUploadTargetRequiredを持つ*UploadTargetErrorです。
	ErrUploadTargetsRequired = errors.New("object storage upload targets are required")
	// ErrInvalidS3Configは、S3を選択したupload先のS3送信先設定が、S3 uploaderの検証規則を
	// 満たさないことを表します。拒否した理由はS3 uploaderのerror（ErrRegionRequiredなど）として、
	// errors.Isやerrors.Asで判別できます。
	ErrInvalidS3Config = errors.New("s3 config is invalid")
)

// UnsupportedUploadTargetErrorは、Scopeの設定に、対応するproviderのないupload targetが
// 指定されたことを表します。errors.Isでは、ErrUnsupportedUploadTargetと一致します。
type UnsupportedUploadTargetError struct {
	// Scopeは、"logger file default"など、targetを指定した設定の位置です。
	// 空の場合、Errorはscopeを表示しません。
	Scope string
	// Targetは、対応するproviderのないupload targetです。
	Target objectstorage.UploadTarget
}

// Errorは、Scopeと未対応のtargetを含む説明を返します。
func (e *UnsupportedUploadTargetError) Error() string {
	if e.Scope == "" {
		return fmt.Sprintf("unsupported object storage upload target: %q", e.Target)
	}
	return fmt.Sprintf("%s unsupported object storage upload target: %q", e.Scope, e.Target)
}

// Isは、targetがErrUnsupportedUploadTargetの場合にtrueを返します。
func (e *UnsupportedUploadTargetError) Is(target error) bool {
	return target == ErrUnsupportedUploadTarget
}

// UploadTargetErrorは、upload targetの一覧のIndex番目にあるTargetが、表記または一意性の
// 規則を満たさないことを表します。規則の種類はErrで、errors.Isで判別できます。
type UploadTargetError struct {
	// Indexは、不正なtargetの一覧内の位置です。
	Index int
	// Targetは、指定されたままのupload targetです。
	Target objectstorage.UploadTarget
	// Canonicalは、ErrNonCanonicalUploadTargetの場合に、Targetの前後の空白を除いて
	// 小文字にした表記です。それ以外では空です。
	Canonical objectstorage.UploadTarget
	// Errは、ErrUploadTargetRequired、ErrNonCanonicalUploadTarget、
	// ErrDuplicateUploadTargetのいずれかです。
	Err error
}

// Errorは、規則の種類に応じて、targetの位置と値を含む説明を返します。
func (e *UploadTargetError) Error() string {
	switch e.Err {
	case ErrUploadTargetRequired:
		return fmt.Sprintf("object storage upload target is required at index %d", e.Index)
	case ErrNonCanonicalUploadTarget:
		return fmt.Sprintf(
			"object storage upload target %q at index %d must use canonical form %q",
			e.Target,
			e.Index,
			e.Canonical,
		)
	case ErrDuplicateUploadTarget:
		return fmt.Sprintf("duplicate object storage upload target %q at index %d", e.Target, e.Index)
	default:
		return fmt.Sprintf("object storage upload target %q at index %d is invalid: %v", e.Target, e.Index, e.Err)
	}
}

// Unwrapは、規則の種類を表すErrを返します。
func (e *UploadTargetError) Unwrap() error { return e.Err }
