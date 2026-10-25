// Package errortreeは、errors.Joinや%wでまとめたerror treeから型付きerrorを探す試験用helperです。
package errortree

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// Allは、errのerror treeに含まれる型Tのerrorを、errors.Isと同じ深さ優先の順で全て返します。
// errors.Asは最初の一件だけを返すため、検証errorをまとめたerrorから設定元や項目ごとの
// errorを列挙する試験で使います。errがnilなら空です。
func All[T error](err error) []T {
	var found []T
	var visit func(error)
	visit = func(err error) {
		if err == nil {
			return
		}
		if typed, ok := err.(T); ok {
			found = append(found, typed)
		}
		switch wrapped := err.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				visit(child)
			}
		case interface{ Unwrap() error }:
			visit(wrapped.Unwrap())
		}
	}
	visit(err)
	return found
}

// Findは、errのerror treeに含まれる型Tのerrorのうち、matchがtrueを返す最初のerrorを返します。
// 見つからなければfalseです。
func Find[T error](err error, match func(T) bool) (T, bool) {
	for _, typed := range All[T](err) {
		if match(typed) {
			return typed, true
		}
	}
	var zero T
	return zero, false
}

// ExportedFieldContainingは、errのerror treeに含まれる各errorの公開field（埋め込みで昇格する
// 公開fieldを含む）を、pointer・interface・struct・slice・array・mapを辿って調べ、substringを
// 含む文字列またはbyte列があればその場所を返します。errors.Asで取り出した呼出側が値として
// 読めるものだけを対象にし、表示文と非公開fieldは調べません。保存されたrecordの値などを、
// 型付きerrorのfieldや包んだ原因として取り出せないことを確認する試験で使います。
func ExportedFieldContaining(err error, substring string) (string, bool) {
	for _, node := range All[error](err) {
		scan := exportedValueScan{substring: substring, visited: map[uintptr]bool{}}
		if location, ok := scan.find(reflect.ValueOf(node), fmt.Sprintf("%T", node), 0); ok {
			return location, true
		}
	}
	return "", false
}

// ExtractableValueは、errからerrors.Asで値を取り出せる場所にvalueが残っていればその場所を返します。
// decode・解析のerror（*json.SyntaxError、*json.UnmarshalTypeError、*time.ParseError）はdecodeした
// 入力の一部を保持するため、error treeに含まれるだけで対象とします。そのほかは
// ExportedFieldContainingと同じく公開fieldを調べます。表示文は調べません。
func ExtractableValue(err error, value string) (string, bool) {
	for _, node := range All[error](err) {
		switch node.(type) {
		case *json.SyntaxError, *json.UnmarshalTypeError, *time.ParseError:
			return fmt.Sprintf("%T", node), true
		}
	}
	return ExportedFieldContaining(err, value)
}

type exportedValueScan struct {
	substring string
	visited   map[uintptr]bool
}

// maxExportedValueDepthは、自己参照を含む値を辿り続けないための上限です。
const maxExportedValueDepth = 64

func (s exportedValueScan) find(value reflect.Value, path string, depth int) (string, bool) {
	if depth > maxExportedValueDepth {
		return "", false
	}
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() || s.visited[value.Pointer()] {
			return "", false
		}
		s.visited[value.Pointer()] = true
		return s.find(value.Elem(), path, depth+1)
	case reflect.Interface:
		if value.IsNil() {
			return "", false
		}
		return s.find(value.Elem(), path, depth+1)
	case reflect.Struct:
		for index := range value.NumField() {
			field := value.Type().Field(index)
			// 非公開fieldは呼出側から読めません。非公開型の埋め込みは、昇格する公開fieldだけを辿ります。
			if !field.IsExported() && !field.Anonymous {
				continue
			}
			if location, ok := s.find(value.Field(index), path+"."+field.Name, depth+1); ok {
				return location, true
			}
		}
	case reflect.Slice, reflect.Array:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			raw := make([]byte, value.Len())
			for index := range value.Len() {
				raw[index] = byte(value.Index(index).Uint())
			}
			if strings.Contains(string(raw), s.substring) {
				return path, true
			}
			return "", false
		}
		for index := range value.Len() {
			if location, ok := s.find(value.Index(index), fmt.Sprintf("%s[%d]", path, index), depth+1); ok {
				return location, true
			}
		}
	case reflect.Map:
		entries := value.MapRange()
		for entries.Next() {
			if location, ok := s.find(entries.Key(), path+"[key]", depth+1); ok {
				return location, true
			}
			if location, ok := s.find(entries.Value(), fmt.Sprintf("%s[%v]", path, entries.Key()), depth+1); ok {
				return location, true
			}
		}
	case reflect.String:
		if strings.Contains(value.String(), s.substring) {
			return path, true
		}
	}
	return "", false
}
