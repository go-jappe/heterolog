package lifecycle

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// processing fileを分割してterminal stateへ書き込んだfileは、元のprocessing fileと
// 同じ相対pathを保ちます。その元fileが存在する間、保存した結果は再起動時のreplayを
// 抑止するために必要な証拠です。recovery workerより先にretentionが実行されても
// 保持します。部分的な元fileや矛盾する元fileは運用者の確認が必要なため、これも保護します。
func terminalHasProcessingSource(stateDir, terminalPath string) (bool, error) {
	switch pkgspool.State(filepath.Base(stateDir)) {
	case pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine:
	default:
		return false, nil
	}
	rel, err := filepath.Rel(stateDir, terminalPath)
	if err != nil {
		return false, err
	}
	root := filepath.Dir(stateDir)
	parentRel := filepath.Join(string(pkgspool.StateProcessing), filepath.Dir(rel))
	if _, err := pathidentity.ResolveAndValidateManagedDirectory(root, parentRel); err != nil {
		return false, fmt.Errorf("verify pending terminal source directory: %w", err)
	}
	_, err = os.Lstat(filepath.Join(root, string(pkgspool.StateProcessing), rel))
	if errors.Is(err, os.ErrNotExist) {
		// 以前のunlinkが、directoryの同期をせずに成功している可能性があります。Runnerは
		// 後始末を再試行する前にpurgeするため、unlinkを安全にしたterminalの証拠を削除する
		// 前に、元fileがないことを確定させます。
		return false, syncAbsentProcessingParent(root, filepath.Join(root, parentRel))
	}
	if err != nil {
		return false, fmt.Errorf("verify pending terminal source: %w", err)
	}
	return true, nil
}

func syncAbsentProcessingParent(root, directory string) error {
	for {
		info, err := os.Lstat(directory)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("processing parent is not a directory: %s", directory)
			}
			return infrafsstore.SyncDirectory(directory)
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if directory == filepath.Clean(root) {
			return nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return err
		}
		directory = parent
	}
}
