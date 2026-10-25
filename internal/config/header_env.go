package config

import pkgenv "github.com/go-jappe/heterolog/pkg/env"

// loadHeaderEnvは、HTTP headerを表すJSON object（header名から値への対応）を環境変数keyから
// 読み込みます。未設定・空の場合はdefaultValueを返します。headerにはAuthorizationなどの
// 認証情報が含まれ得るため、値を変換できない場合のerrorはkeyとenv.ErrInvalidJSONだけを保持します。
// pkg/envのVariableErrorとJSON parserの診断は値やその一部を含み得るため包まず、値はerror treeの
// どこにも残しません（*_S3_DESTINATIONSと同じ規則です）。errorの場合もdefaultValueを返します。
func loadHeaderEnv(env pkgenv.Lookup, key string, defaultValue map[string]string) (map[string]string, error) {
	headers, err := pkgenv.GetEnvJSONFrom(env, key, defaultValue, false)
	if err != nil {
		return defaultValue, newInvalidEnvironmentValueError(
			key,
			"",
			pkgenv.ErrInvalidJSON,
			"environment variable %q must contain a JSON object of string header values; the value is not shown because it can contain credentials",
			key,
		)
	}
	return headers, nil
}
