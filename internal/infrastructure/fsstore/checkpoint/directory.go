package checkpoint

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// PrepareDirectoryは、loggerや周期処理を開始する前に、共通の
// checkpoint保存先を用意します。uploadを使用するloggerの構築中に一度呼び出します。
// 同じ保存先を準備する呼び出しは並行させず、順番に行ってください。
// 親のリンクは実体へ解決し、保存先自身のリンクは拒否します。作成・権限設定・親の同期に
// 失敗した場合は初期化エラーを返します。checkpointファイル本体は作成しません。
// checkpoint保存や周期処理からは呼びません。top-level loggerを使わず、Storeなどの
// 下位部品だけを直接利用する場合は、呼び出し元が先に準備してください。
func PrepareDirectory(directory string) error {
	if strings.TrimSpace(directory) == "" {
		return errors.New("archive checkpoint directory is empty")
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return fmt.Errorf("resolve archive checkpoint directory %q: %w", directory, err)
	}
	resolved, err := pathidentity.ResolveAndValidateManagedDirectory(filepath.Dir(absolute), filepath.Base(absolute))
	if err != nil {
		return fmt.Errorf("resolve archive checkpoint directory %q: %w", directory, err)
	}
	if err := fsstore.EnsureDirectory(resolved, 0o700); err != nil {
		return fmt.Errorf("prepare archive checkpoint directory %q: %w", directory, err)
	}
	return nil
}
