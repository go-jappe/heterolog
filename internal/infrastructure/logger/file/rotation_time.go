package file

import (
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
)

// updateActiveFilePathは、rotation判定と共有するfileOperationTimeを使ってactive pathを
// 解決し、FileNameの日付・indexとActiveFilePathを同じfileへ揃えます。
func updateActiveFilePath(
	store *appendlog.FileStore,
	cfg logpattern.ConfigProvider,
	fileOperationTime time.Time,
) error {
	path, err := logpattern.ResolveFilePathAt(&store.FileName, cfg, fileOperationTime)
	if err != nil {
		return err
	}
	store.ActiveFilePath = path
	return nil
}
