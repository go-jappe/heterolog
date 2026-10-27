package config

import (
	"errors"
	"sort"

	"github.com/go-jappe/heterolog/internal/config/renamedkey"
	pkgenv "github.com/go-jappe/heterolog/pkg/env"
)

// validateRenamedEnvironmentKeysは、keysに旧名のkeyが残っていないことを確認します。
// LoadConfigはprocess environmentと環境設定fileの両方のkey名を渡すため、どちらに残っていても
// 検出できます。旧名と現在のkey名の対応はrenamedkeyに定義しています。
//
// 旧名を新しいkeyの別名としては読みません。黙って無視すると既存の.envの設定が失われる
// ため、値が空の場合も含め、新しいkey名を示す設定エラーにします。
func validateRenamedEnvironmentKeys(keys []string) error {
	var renamed []string
	for _, key := range keys {
		if _, ok := renamedkey.Current(key); ok {
			renamed = append(renamed, key)
		}
	}
	sort.Strings(renamed)

	errs := make([]error, 0, len(renamed))
	for _, key := range renamed {
		renamedKey, _ := renamedkey.Current(key)
		errs = append(errs, &pkgenv.RenamedEnvironmentKeyError{Old: key, New: renamedKey})
	}
	return errors.Join(errs...)
}
