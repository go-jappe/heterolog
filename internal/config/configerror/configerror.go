// Package configerrorは、設定の検証errorに種類（sentinel）を付けるための補助です。
// 表示文は従来のfmt.Errorfと同じまま、errors.Isで種類を、errors.Is/Asで%wの原因を
// 辿れるerrorを作ります。設定package間で共有するため、他のpackageに依存しない
// leaf packageにしています。
package configerror

import "fmt"

// Newは、formatとargsからfmt.Errorfと同じ表示文のerrorを作り、kindをerrors.Isで
// 判定できるようにします。formatの%wで包んだ原因も、従来どおりerrors.Is/Asで辿れます。
func New(kind error, format string, args ...any) error {
	return &kindError{kind: kind, err: fmt.Errorf(format, args...)}
}

type kindError struct {
	kind error
	err  error
}

func (e *kindError) Error() string { return e.err.Error() }

func (e *kindError) Unwrap() []error { return []error{e.kind, e.err} }
