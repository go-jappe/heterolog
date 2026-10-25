package objectstoragespool

import (
	"context"
	"errors"
	"fmt"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
)

// CountPreservedFilesは全宛先の保全対象件数を返します。
// 元データのコピー、同名衝突によるrename、不正項目の隔離を、各宛先の.preservedから集計します。
// リンクやディレクトリも直下の項目ごとに1件と数え、リンク先や中身はたどりません。
// 未登録宛先の保全対象も数え、再起動後の周期通知に含めます。
func (s *DestinationStores) CountPreservedFiles(ctx context.Context) (int, error) {
	stores, scanErr := s.Existing(ctx)
	count := 0
	var firstErr error
	invalidStores := 0
	for _, store := range stores {
		found, err := infrafsstore.CountPreservedFiles(ctx, store.Root().Path())
		count += found
		if err != nil {
			invalidStores++
			if firstErr == nil {
				firstErr = fmt.Errorf("destination %q: %w", store.Root().DestinationID(), err)
			}
		}
	}
	if invalidStores != 0 {
		firstErr = fmt.Errorf("%d destination spool directories have invalid preserved entries; first: %w", invalidStores, firstErr)
	}
	return count, errors.Join(scanErr, firstErr)
}
