package failurestore

import (
	"errors"
	"fmt"
)

var (
	// ErrDurableTypesRequiredは、durable storeの選択が一つもないことを表します。
	ErrDurableTypesRequired = errors.New("durable types are required")
	// ErrDurableTypeNoneNotAloneは、DurableTypeNoneがほかのdurable typeと同時に
	// 選択されたことを表します。
	ErrDurableTypeNoneNotAlone = errors.New(`durable type "none" must be specified alone`)
	// ErrUnsupportedDurableTypeは、scopeが対応しないdurable typeが選択されたことを表します。
	ErrUnsupportedDurableType = errors.New("unsupported durable type")
	// ErrDuplicateDurableTypeは、同じdurable typeが複数回選択されたことを表します。
	ErrDuplicateDurableType = errors.New("duplicate durable type")
)

// DurableTypeErrorは、Scopeのdurable store選択が不正であることを表します。
// 不正の種類はErrで、errors.Isで判別できます。
type DurableTypeError struct {
	// Scopeは、"delivery failure store"など、durable store選択を持つ設定の範囲です。
	Scope string
	// Typeは不正なdurable typeです。ErrDurableTypesRequiredでは空です。
	Type DurableType
	// Errは、ErrDurableTypesRequired、ErrDurableTypeNoneNotAlone、
	// ErrUnsupportedDurableType、ErrDuplicateDurableTypeのいずれかです。
	Err error
}

// Errorは、不正の種類に応じて、ScopeとTypeを含む説明を返します。
func (e *DurableTypeError) Error() string {
	switch e.Err {
	case ErrDurableTypesRequired:
		return fmt.Sprintf("%s durable types are required", e.Scope)
	case ErrDurableTypeNoneNotAlone:
		return fmt.Sprintf("%s durable type %q must be specified alone", e.Scope, e.Type)
	case ErrUnsupportedDurableType:
		return fmt.Sprintf("unsupported %s durable type: %q", e.Scope, e.Type)
	case ErrDuplicateDurableType:
		return fmt.Sprintf("duplicate %s durable type: %q", e.Scope, e.Type)
	default:
		return fmt.Sprintf("invalid %s durable type %q: %v", e.Scope, e.Type, e.Err)
	}
}

// Unwrapは、不正の種類を表すErrを返します。
func (e *DurableTypeError) Unwrap() error { return e.Err }
