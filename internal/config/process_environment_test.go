package config

import pkgenv "github.com/go-jappe/heterolog/pkg/env"

// processEnvironmentForTestは、試験がt.Setenvなどで設定したprocess environmentを読むLookupを
// 返します。loaderを直接呼ぶ試験で使います。
func processEnvironmentForTest() pkgenv.Lookup {
	return pkgenv.ProcessEnvironment()
}

// fromProcessEnvironmentは、loadをprocess environmentで呼ぶ関数を返します。loaderを試験の表に
// 並べる場合に使います。
func fromProcessEnvironment[T any](load func(pkgenv.Lookup) (T, error)) func() (T, error) {
	return func() (T, error) { return load(processEnvironmentForTest()) }
}
