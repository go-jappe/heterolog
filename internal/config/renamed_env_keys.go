package config

import (
	"errors"
	"fmt"
	"sort"

	"github.com/go-jappe/heterolog/internal/config/renamedkey"
)

// RenamedEnvironmentKeyErrorは、綴りを修正した旧名のkeyが環境変数または環境設定fileに
// 残っていることを表します。Oldは残っている旧名、Newは現在のkey名です。
// LoadConfigは旧名を新しいkeyの別名として読まず、このerrorを返します。
// 複数の旧名が残る場合はerrors.Joinでまとめるため、errors.Asで取り出してください。
type RenamedEnvironmentKeyError struct {
	Old string
	New string
}

func (e *RenamedEnvironmentKeyError) Error() string {
	return fmt.Sprintf(
		"environment variable %q was renamed to %q; rename it in the environment or env file",
		e.Old,
		e.New,
	)
}

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
		errs = append(errs, &RenamedEnvironmentKeyError{Old: key, New: renamedKey})
	}
	return errors.Join(errs...)
}
