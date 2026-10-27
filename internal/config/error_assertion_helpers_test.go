package config

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	mappedvalidation "github.com/go-jappe/heterolog/internal/config/validation/mapped"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	"github.com/go-jappe/heterolog/pkg/env"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// environmentErrorKeysは、errに含まれる環境変数の値のerrorのkeyを返します。pkg/envの
// VariableErrorと、loaderが値を解釈するInvalidEnvironmentValueErrorの両方を対象にします。
func environmentErrorKeys(err error) []string {
	var keys []string
	for _, variableErr := range errortree.All[*env.VariableError](err) {
		keys = append(keys, variableErr.Key)
	}
	for _, valueErr := range errortree.All[*env.InvalidEnvironmentValueError](err) {
		keys = append(keys, valueErr.Key)
	}
	return keys
}

// environmentKeyErrorは、errに含まれるkeyの環境変数の値のerrorを返します。見つからなければnilです。
func environmentKeyError(err error, key string) error {
	for _, variableErr := range errortree.All[*env.VariableError](err) {
		if variableErr.Key == key {
			return variableErr
		}
	}
	for _, valueErr := range errortree.All[*env.InvalidEnvironmentValueError](err) {
		if valueErr.Key == key {
			return valueErr
		}
	}
	return nil
}

// requireEnvironmentKeyErrorは、errがkeyの値を拒否したerrorを含み、kindがnilでなければ
// そのerrorがerrors.Isでkindと一致することを確認します。
func requireEnvironmentKeyError(t *testing.T, err error, key string, kind error) {
	t.Helper()
	keyErr := environmentKeyError(err, key)
	if keyErr == nil {
		t.Fatalf("error = %v, want an invalid value error for %s (found keys %v)", err, key, environmentErrorKeys(err))
	}
	if kind != nil && !errors.Is(keyErr, kind) {
		t.Fatalf("%s error = %v, want %v", key, keyErr, kind)
	}
}

// requireEnvironmentKeyErrorsは、errが各keyの値を拒否したerrorを全て含むことを確認します。
func requireEnvironmentKeyErrors(t *testing.T, err error, keys ...string) {
	t.Helper()
	found := environmentErrorKeys(err)
	for _, key := range keys {
		if !slices.Contains(found, key) {
			t.Fatalf("error = %v, want an invalid value error for %s (found keys %v)", err, key, found)
		}
	}
}

// requireMappedConfigErrorは、errが変換済み設定の検証でsourceのsection設定を拒否した
// mappedvalidation.ConfigErrorを含み、kindがnilでなければその原因がerrors.Isでkindと
// 一致することを確認して、そのerrorを返します。
func requireMappedConfigError(
	t *testing.T,
	err error,
	source string,
	section mappedvalidation.ConfigSection,
	kind error,
) *mappedvalidation.ConfigError {
	t.Helper()
	configErr, ok := errortree.Find(err, func(configErr *mappedvalidation.ConfigError) bool {
		return configErr.Source == source && configErr.Section == section
	})
	if !ok {
		var found []string
		for _, configErr := range errortree.All[*mappedvalidation.ConfigError](err) {
			found = append(found, fmt.Sprintf("%s/%s", configErr.Source, configErr.Section))
		}
		t.Fatalf("error = %v, want %s %s config error (found %v)", err, source, section, found)
	}
	if kind != nil && !errors.Is(configErr, kind) {
		t.Fatalf("%s %s config error = %v, want %v", source, section, configErr, kind)
	}
	return configErr
}

// requireLoggerConfigFieldErrorは、errがsourceのbackend設定のfieldをkindの理由で拒否した
// pkg/loggerのConfigFieldErrorを含むことを確認して、そのerrorを返します。
func requireLoggerConfigFieldError(
	t *testing.T,
	err error,
	source string,
	backend pkglogger.LogOutputDestination,
	field pkglogger.ConfigField,
	kind error,
) *pkglogger.ConfigFieldError {
	t.Helper()
	var found []string
	for _, fieldErr := range pkglogger.ConfigFieldErrors(err) {
		if fieldErr.Source == source && fieldErr.Backend == backend && fieldErr.Field == field &&
			errors.Is(fieldErr, kind) {
			return fieldErr
		}
		found = append(found, fmt.Sprintf("%s/%s/%s", fieldErr.Source, fieldErr.Backend, fieldErr.Field))
	}
	t.Fatalf("error = %v, want %s %s %s %v (found %v)", err, source, backend, field, kind, found)
	return nil
}

// loggerSettingErrorは、pkg/loggerのConfigFieldErrorとして期待する不正な設定です。
// valueとlimitがnilでなければ、拒否した値と比較した関連設定の値も確認します。
type loggerSettingError struct {
	source  string
	backend pkglogger.LogOutputDestination
	field   pkglogger.ConfigField
	kind    error
	value   any
	limit   any
}

// requireLoggerSettingErrorは、errがwantの設定を拒否したConfigFieldErrorを含むことを確認します。
func requireLoggerSettingError(t *testing.T, err error, want loggerSettingError) {
	t.Helper()
	fieldErr := requireLoggerConfigFieldError(t, err, want.source, want.backend, want.field, want.kind)
	if want.value != nil && fieldErr.Value != want.value {
		t.Fatalf("%s %s %s error value = %v, want %v", want.source, want.backend, want.field, fieldErr.Value, want.value)
	}
	if want.limit != nil && fieldErr.Limit != want.limit {
		t.Fatalf("%s %s %s error limit = %v, want %v", want.source, want.backend, want.field, fieldErr.Limit, want.limit)
	}
}

// requireNoValueInErrorTreeは、errのerror treeに含まれるどのerrorも、表示文と文字列のfield
// （非公開のfieldを含む）にvalueを含まないことを確認します。認証情報を含み得る設定値を、
// 表示文だけでなくerrors.Asで取り出せる型付きerrorのValueや包んだ原因にも残さないことが
// 要件のため、表示文も対象にします。
func requireNoValueInErrorTree(t *testing.T, err error, value string) {
	t.Helper()
	for _, node := range errortree.All[error](err) {
		if strings.Contains(node.Error(), value) {
			t.Fatalf("%T error text keeps the configured value", node)
		}
		if field, ok := stringFieldContaining(reflect.ValueOf(node), fmt.Sprintf("%T", node), value); ok {
			t.Fatalf("%s keeps the configured value", field)
		}
	}
}

// stringFieldContainingは、valueがpointer・interface・struct・slice・arrayを辿って到達できる
// 文字列にsubstringを含む場合、その場所を返します。
func stringFieldContaining(value reflect.Value, path, substring string) (string, bool) {
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface:
		if value.IsNil() {
			return "", false
		}
		return stringFieldContaining(value.Elem(), path, substring)
	case reflect.Struct:
		for index := range value.NumField() {
			if field, ok := stringFieldContaining(value.Field(index), path+"."+value.Type().Field(index).Name, substring); ok {
				return field, true
			}
		}
	case reflect.Slice, reflect.Array:
		for index := range value.Len() {
			if field, ok := stringFieldContaining(value.Index(index), fmt.Sprintf("%s[%d]", path, index), substring); ok {
				return field, true
			}
		}
	case reflect.String:
		if strings.Contains(value.String(), substring) {
			return path, true
		}
	}
	return "", false
}
