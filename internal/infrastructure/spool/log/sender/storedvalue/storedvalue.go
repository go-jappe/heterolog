// Package storedvalueは、spoolに保存されたrecordの値を保持し得る検証errorを、表示文と
// 種類のsentinelだけを残したerrorへ置き換えます。保存されたrecordの値は、errors.Asで
// 取り出せる型付きerrorのfieldや原因に残しません。
package storedvalue

import "errors"

// WithoutValuesは、errの表示文を保ち、kindsのうちerrがerrors.Isで一致するものだけを
// errors.Isで判定できるerrorを返します。errが保持する型付きerror（保存された値を
// fieldに持つものを含む）は、errors.Asで取り出せません。kindsには値を持たないsentinelを
// 渡してください。errがnilならnilを返します。
func WithoutValues(err error, kinds ...error) error {
	if err == nil {
		return nil
	}
	matched := make([]error, 0, len(kinds))
	for _, kind := range kinds {
		if kind != nil && errors.Is(err, kind) {
			matched = append(matched, kind)
		}
	}
	return &valueFreeError{message: err.Error(), kinds: matched}
}

// valueFreeErrorは、元のerrorの表示文と、一致した種類のsentinelだけを持ちます。
type valueFreeError struct {
	message string
	kinds   []error
}

func (e *valueFreeError) Error() string { return e.message }

func (e *valueFreeError) Unwrap() []error { return e.kinds }
