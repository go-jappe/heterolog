package mappedvalidation

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// configErrorLabelsは、errに含まれるConfigErrorの設定元と区分を、失敗時の表示用に返します。
func configErrorLabels(err error) []string {
	var labels []string
	for _, configErr := range errortree.All[*ConfigError](err) {
		labels = append(labels, fmt.Sprintf("%s/%s", configErr.Source, configErr.Section))
	}
	return labels
}

// findConfigErrorは、errに含まれるsourceとsectionのConfigErrorを返します。
func findConfigError(err error, source string, section ConfigSection) (*ConfigError, bool) {
	return errortree.Find(err, func(configErr *ConfigError) bool {
		return configErr.Source == source && configErr.Section == section
	})
}

// requireConfigErrorは、errがsourceのsection設定を拒否したConfigErrorを含み、kindがnilでなければ
// その原因がerrors.Isでkindと一致することを確認します。
func requireConfigError(t *testing.T, err error, source string, section ConfigSection, kind error) *ConfigError {
	t.Helper()
	configErr, ok := findConfigError(err, source, section)
	if !ok {
		t.Fatalf("error = %v, want %s %s ConfigError (found %v)", err, source, section, configErrorLabels(err))
	}
	if kind != nil && !errors.Is(configErr, kind) {
		t.Fatalf("%s %s error = %v, want %v", source, section, configErr, kind)
	}
	return configErr
}

// findConfigFieldErrorは、errに含まれる、sourceのbackend設定のfieldを拒否したpkg/loggerの
// ConfigFieldErrorを返します。
func findConfigFieldError(
	err error,
	source string,
	backend pkglogger.LogOutputDestination,
	field pkglogger.ConfigField,
) (*pkglogger.ConfigFieldError, bool) {
	for _, fieldErr := range pkglogger.ConfigFieldErrors(err) {
		if fieldErr.Source == source && fieldErr.Backend == backend && fieldErr.Field == field {
			return fieldErr, true
		}
	}
	return nil, false
}

// requireConfigFieldErrorは、errがsourceのbackend設定のfieldを拒否したpkg/loggerの
// ConfigFieldErrorを含み、その種類がerrors.Isでkindと一致することを確認して、そのerrorを返します。
func requireConfigFieldError(
	t *testing.T,
	err error,
	source string,
	backend pkglogger.LogOutputDestination,
	field pkglogger.ConfigField,
	kind error,
) *pkglogger.ConfigFieldError {
	t.Helper()
	fieldErr, ok := findConfigFieldError(err, source, backend, field)
	if !ok {
		var found []string
		for _, fieldErr := range pkglogger.ConfigFieldErrors(err) {
			found = append(found, fmt.Sprintf("%s/%s/%s", fieldErr.Source, fieldErr.Backend, fieldErr.Field))
		}
		t.Fatalf("error = %v, want %s %s %s ConfigFieldError (found %v)", err, source, backend, field, found)
	}
	if !errors.Is(fieldErr, kind) {
		t.Fatalf("%s %s %s error = %v, want %v", source, backend, field, fieldErr, kind)
	}
	return fieldErr
}
