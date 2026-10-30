// Package configviewは、loggerとtelemetryを作った設定を、管理用APIが返すJSONの文書に
// します。設定の構造体をそのまま写し、鍵やtokenなどの秘密の値は伏せます。文書の組み立て
// だけを受け持ち、設定の保持と検証は、設定の持ち主（loggerruntimeなど）とpkg/の検証が
// 行います。
package configview

import (
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode"
)

// Maskedは、秘密の値の代わりに文書へ入れる文字列です。空の秘密の値は空のままにし、
// 設定されているかどうかだけが分かるようにします。
const Masked = "***"

// modulePathは、このmoduleのimport pathです。interfaceに入った値は、このmoduleの型の
// ものだけを写します。利用者が渡した他のmoduleの実装（AWSの認証情報providerなど）は、
// 中身が分からず秘密を含み得るため、型の名前だけにします。
const modulePath = "github.com/go-jappe/heterolog/"

// secretFieldNamesは、値を伏せるfieldの名前です。名前が一致する文字列のfieldは、どの
// 設定の型にあっても伏せます。設定の型に秘密のfieldを足すときは、この一覧に名前を足し
// ます。render_test.goが、設定の型のそれらしい名前のfieldがここにあることを確かめます。
var secretFieldNames = map[string]bool{
	"Token":           true,
	"BearerToken":     true,
	"Password":        true,
	"Secret":          true,
	"AccessKeyID":     true,
	"SecretAccessKey": true,
	"SessionToken":    true,
}

// maxDepthは、写す構造体の深さの上限です。設定の型は10段ほどの深さで、上限に届くのは
// 利用者の型が自身を参照する場合だけです。
const maxDepth = 64

var (
	stringerType = reflect.TypeFor[fmt.Stringer]()
	timeType     = reflect.TypeFor[time.Time]()
)

// Renderは、設定の値をJSONにできる値（map[string]any・[]any・文字列・数・bool・nil）に
// します。
//
//   - 構造体は、公開されたfieldをsnake_caseの名前（HTTPClientはhttp_client）のobjectに
//     します。関数・channelのfieldと、公開されたfieldを持たない構造体のfield
//     （LiveSettingsなど）は省きます。
//   - secretFieldNamesの名前の文字列のfieldは、空でなければMaskedにします。
//   - time.Durationは"1m30s"のような文字列、time.TimeはRFC 3339の文字列にします。
//     ほかの整数の型でfmt.Stringerを実装するもの（LogLevelなど）も、String()の文字列に
//     します。
//   - nilのpointerとinterfaceはnullにします。interfaceの値は、このmoduleの型なら写し、
//     ほかのmoduleの型なら型の名前の文字列にします。
//   - mapはkeyを文字列にしたobject、sliceと配列は配列にします。
//
// 同じpointerを辿る循環は、一度写した先をnullにして止めます。
func Render(value any) any {
	rendered, _ := render(reflect.ValueOf(value), "", 0, map[uintptr]bool{})
	return rendered
}

// renderは、値を写します。省くfield（関数など）はomitted=falseで知らせます。
func render(v reflect.Value, fieldName string, depth int, visiting map[uintptr]bool) (rendered any, present bool) {
	if !v.IsValid() {
		return nil, true
	}
	if depth > maxDepth {
		return nil, true
	}
	if secretFieldNames[fieldName] && v.Kind() == reflect.String {
		if v.Len() == 0 {
			return "", true
		}
		return Masked, true
	}
	switch v.Kind() {
	case reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return nil, false
	case reflect.Pointer:
		if v.IsNil() {
			return nil, true
		}
		address := v.Pointer()
		if visiting[address] {
			return nil, true
		}
		visiting[address] = true
		defer delete(visiting, address)
		return render(v.Elem(), fieldName, depth+1, visiting)
	case reflect.Interface:
		if v.IsNil() {
			return nil, true
		}
		elem := v.Elem()
		if !isModuleType(elem.Type()) {
			return elem.Type().String(), true
		}
		return render(elem, fieldName, depth+1, visiting)
	case reflect.Struct:
		if v.Type() == timeType {
			return v.Interface().(time.Time).Format(time.RFC3339Nano), true
		}
		return renderStruct(v, depth, visiting)
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return nil, true
		}
		items := make([]any, 0, v.Len())
		for i := range v.Len() {
			item, ok := render(v.Index(i), "", depth+1, visiting)
			if ok {
				items = append(items, item)
			}
		}
		return items, true
	case reflect.Map:
		if v.IsNil() {
			return nil, true
		}
		object := make(map[string]any, v.Len())
		iter := v.MapRange()
		for iter.Next() {
			item, ok := render(iter.Value(), "", depth+1, visiting)
			if ok {
				object[fmt.Sprint(iter.Key().Interface())] = item
			}
		}
		return object, true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if v.Type().Implements(stringerType) {
			return v.Interface().(fmt.Stringer).String(), true
		}
		if v.CanInt() {
			return v.Int(), true
		}
		return v.Uint(), true
	case reflect.Float32, reflect.Float64:
		return v.Float(), true
	case reflect.Bool:
		return v.Bool(), true
	case reflect.String:
		return v.String(), true
	default:
		// complexなど、設定にない種類です。
		return fmt.Sprint(v.Interface()), true
	}
}

// renderStructは、構造体の公開されたfieldを写します。写すfieldが一つもなければ
// present=falseを返し、親はそのfieldを省きます。
func renderStruct(v reflect.Value, depth int, visiting map[uintptr]bool) (any, bool) {
	t := v.Type()
	object := map[string]any{}
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		rendered, ok := render(v.Field(i), field.Name, depth+1, visiting)
		if !ok {
			continue
		}
		if field.Anonymous && isStructOrPointerToStruct(field.Type) {
			// 埋め込んだ構造体のfieldは、Goと同じく親のfieldとして見せます。
			if embedded, isObject := rendered.(map[string]any); isObject {
				for key, value := range embedded {
					if _, taken := object[key]; !taken {
						object[key] = value
					}
				}
				continue
			}
		}
		object[JSONKey(field.Name)] = rendered
	}
	if len(object) == 0 {
		return nil, false
	}
	return object, true
}

func isStructOrPointerToStruct(t reflect.Type) bool {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Kind() == reflect.Struct
}

// isModuleTypeは、tがこのmoduleで定義した型（pointerならその先の型）かを返します。
func isModuleType(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return strings.HasPrefix(t.PkgPath(), modulePath)
}

// JSONKeyは、Goのfieldの名前をJSONのkeyにします。大文字の並びは一語として扱います
// （HTTPClientはhttp_client、AccessKeyIDはaccess_key_id、S3Bucketはs3_bucket）。
func JSONKey(fieldName string) string {
	runes := []rune(fieldName)
	var b strings.Builder
	for i, r := range runes {
		if i > 0 && unicode.IsUpper(r) {
			previousLower := unicode.IsLower(runes[i-1]) || unicode.IsDigit(runes[i-1])
			nextLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
			if previousLower || (unicode.IsUpper(runes[i-1]) && nextLower) {
				b.WriteByte('_')
			}
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

// Lookupは、Renderの文書からpathの先の値を返します。pathの各要素は、objectのkeyか配列の
// 添字（0から）です。pathが空なら文書そのものを返し、辿れなければfalseを返します。
func Lookup(document any, path []string) (any, bool) {
	current := document
	for _, segment := range path {
		switch node := current.(type) {
		case map[string]any:
			next, ok := node[segment]
			if !ok {
				return nil, false
			}
			current = next
		case []any:
			index, ok := arrayIndex(segment, len(node))
			if !ok {
				return nil, false
			}
			current = node[index]
		default:
			return nil, false
		}
	}
	return current, true
}

// arrayIndexは、segmentを0以上length未満の添字として読みます。先頭の0や符号は認めません。
func arrayIndex(segment string, length int) (int, bool) {
	if segment == "" || (len(segment) > 1 && segment[0] == '0') {
		return 0, false
	}
	index := 0
	for _, r := range segment {
		if r < '0' || r > '9' {
			return 0, false
		}
		index = index*10 + int(r-'0')
		if index >= length {
			return 0, false
		}
	}
	return index, true
}
