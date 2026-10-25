package fsstore

import (
	"errors"
	"fmt"
)

const (
	OverloadPolicyDrop     OverloadPolicy   = "drop"
	OverloadPolicyDegrade  OverloadPolicy   = "degrade"
	SpillDestinationStdout SpillDestination = "stdout"
)

var (
	// ErrUnsupportedOverloadPolicyは、file書込み時の空き容量不足への方針がdrop・degradeの
	// いずれでもないことを表します。ValidateFileOverloadConfigが返すerrorとerrors.Isで判別できます。
	ErrUnsupportedOverloadPolicy = errors.New("unsupported file overload policy")
	// ErrUnsupportedSpillDestinationは、degradeを選んだfile書込みの退避先が、実装済みのstdoutでない
	// ことを表します。ValidateFileOverloadConfigが返すerrorとerrors.Isで判別できます。
	ErrUnsupportedSpillDestination = errors.New("unsupported file overload secondary destination")
)

// ValidateFileOverloadConfigは、通常・asyncのfile書込み時の空き容量不足への対応を検証します。
// policyの未指定はdropです。degradeの退避先はstdoutだけに対応し、drop時の退避先は使用しません。
// 不正な方針はErrUnsupportedOverloadPolicy、degradeの不正な退避先はErrUnsupportedSpillDestinationと
// errors.Isで一致するerrorです。
func ValidateFileOverloadConfig(policy OverloadPolicy, spill SpillDestination) error {
	switch policy {
	case "", OverloadPolicyDrop:
		return nil
	case OverloadPolicyDegrade:
		if spill != SpillDestinationStdout {
			return &overloadConfigError{
				kind:    ErrUnsupportedSpillDestination,
				message: fmt.Sprintf("file overload secondary destination %q is unsupported; degrade requires stdout", spill),
			}
		}
		return nil
	default:
		return &overloadConfigError{
			kind:    ErrUnsupportedOverloadPolicy,
			message: fmt.Sprintf("file overload policy %q is invalid; use drop or degrade", policy),
		}
	}
}

// overloadConfigErrorは、従来の表示文を保ったまま、不正な設定の種類kindをerrors.Isで
// 判別できるようにするerrorです。
type overloadConfigError struct {
	kind    error
	message string
}

func (e *overloadConfigError) Error() string { return e.message }

func (e *overloadConfigError) Unwrap() error { return e.kind }
