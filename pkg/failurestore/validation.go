package failurestore

import (
	"slices"
)

// ValidateDurableTypesは、scopeが対応するdurable storeの選択を検証します。
// noneは単独指定だけを受け付け、それ以外はsupportedに含まれる値を一度ずつ
// 選択できます。不正な選択は*DurableTypeErrorです。
func ValidateDurableTypes(
	scope string,
	types []DurableType,
	supported ...DurableType,
) error {
	if len(types) == 0 {
		return &DurableTypeError{Scope: scope, Err: ErrDurableTypesRequired}
	}
	seen := make(map[DurableType]struct{}, len(types))
	for _, durableType := range types {
		if durableType == DurableTypeNone {
			if len(types) != 1 {
				return &DurableTypeError{Scope: scope, Type: durableType, Err: ErrDurableTypeNoneNotAlone}
			}
		} else if !slices.Contains(supported, durableType) {
			return &DurableTypeError{Scope: scope, Type: durableType, Err: ErrUnsupportedDurableType}
		}
		if _, duplicated := seen[durableType]; duplicated {
			return &DurableTypeError{Scope: scope, Type: durableType, Err: ErrDuplicateDurableType}
		}
		seen[durableType] = struct{}{}
	}
	return nil
}
