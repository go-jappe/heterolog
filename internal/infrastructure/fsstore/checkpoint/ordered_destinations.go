package checkpoint

import (
	"context"
	"fmt"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// ReadyDestinationIDsは、古い未完了世代がない宛先だけを返します。
// observedKeysには圧縮失敗中の元ログも含め、系列の既存ファイルを渡します。
// 圧縮の有無は完了判定に影響せず、同じ世代は一度だけ確認します。完了済みの世代は妨げません。
// 呼び出し元は系列のlifecycle排他中に一覧を取得し、処理中の保護パスも含めます。
// upload中に排他を解放する場合も、完了まではその世代を保護し、新しい世代だけを
// 追加する運用にします。走査後に過去の世代を追加・置換する操作は扱いません。
// Store自身はfilesystemを走査せず、宛先Aの失敗で完了可能なBまで止めません。
func ReadyDestinationIDs(
	ctx context.Context,
	store Store,
	key CheckpointKey,
	pending []objectstorage.DestinationID,
	observedKeys []CheckpointKey,
) ([]objectstorage.DestinationID, error) {
	if err := validateCheckpointGeneration(key.Generation); err != nil {
		return nil, err
	}
	for index, observed := range observedKeys {
		if err := validateCheckpointGeneration(observed.Generation); err != nil {
			return nil, fmt.Errorf("observed file key[%d]: %w", index, err)
		}
	}
	if len(pending) == 0 {
		return nil, nil
	}
	blocked := make(map[objectstorage.DestinationID]struct{})
	seen := make(map[string]struct{}, len(observedKeys))
	for _, earlier := range observedKeys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if CompareGenerations(earlier.Generation, key.Generation) >= 0 {
			continue
		}
		if _, exists := seen[earlier.Generation]; exists {
			continue
		}
		seen[earlier.Generation] = struct{}{}
		state, err := store.Load(ctx, earlier)
		if err != nil {
			return nil, fmt.Errorf("inspect earlier log generation %+v before %+v: %w", earlier, key, err)
		}
		for _, destination := range state.PendingDestinationIDs(pending) {
			blocked[destination] = struct{}{}
		}
	}
	ready := make([]objectstorage.DestinationID, 0, len(pending))
	for _, destination := range pending {
		if _, exists := blocked[destination]; !exists {
			ready = append(ready, destination)
		}
	}
	return ready, nil
}
