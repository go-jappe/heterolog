// Package pathcaseruleは、試験だけがpathidentityのdirectoryの大小文字の規則を差し替えるための
// 内部hookです。本番コードは設定しません。実行するOSやfilesystemに依存せず、大小文字を
// 区別する場合・しない場合の両方を試験するために使います。
//
// 規則はprocess全体に適用されるため、設定中は同じprocessで他の試験を並行して実行しないでください。
// 通常はinternal/testutilのUseDirectoryCaseRuleForTestを使い、試験の終了時に元へ戻します。
package pathcaserule

import (
	"errors"
	"sync/atomic"
)

// Ruleは、directory直下の名前が大小文字を区別するかを返します。
// 規則を差し替えないdirectoryではokにfalseを返し、filesystemの判定へ任せます。
type Rule func(directory string) (sensitive bool, ok bool)

// ErrAlreadySetは、別の規則が設定済みのため新しい規則を設定できないことを表します。
var ErrAlreadySet = errors.New("directory case rule override is already set")

var current atomic.Pointer[Rule]

// Setはruleを設定し、元に戻す関数を返します。同時に設定できる規則は一つです。
func Set(rule Rule) (restore func(), err error) {
	if rule == nil {
		return nil, errors.New("directory case rule override must not be nil")
	}
	installed := &rule
	if !current.CompareAndSwap(nil, installed) {
		return nil, ErrAlreadySet
	}
	return func() { current.CompareAndSwap(installed, nil) }, nil
}

// Activeは、規則が設定されているかを返します。
func Active() bool {
	return current.Load() != nil
}

// Lookupは、設定済みの規則でdirectoryの規則を返します。未設定または対象外ならokがfalseです。
func Lookup(directory string) (sensitive bool, ok bool) {
	rule := current.Load()
	if rule == nil {
		return false, false
	}
	return (*rule)(directory)
}
