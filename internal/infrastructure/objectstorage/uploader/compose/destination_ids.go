package compose

import (
	"fmt"
	"strings"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	objectstoragevalidation "github.com/go-jappe/heterolog/pkg/objectstorage/validation"
)

// DestinationIDResolverはproviderの全宛先を検証し、Uploaderを作らずに宛先ID一覧を返します。
type DestinationIDResolver func(objectstorage.UploadTargetConfigProvider) ([]objectstorage.DestinationID, error)

// DestinationIDsFromTargetsは、起動時のcheckpoint登録に使う宛先IDを設定順で返します。
// 実送信用のBuilderと同じID生成規則を持つresolverを渡してください。
// Uploaderや通信clientは構築せず、不正な設定や空・重複した宛先IDは拒否します。
func DestinationIDsFromTargets(
	cfg objectstorage.UploadTargetConfigProvider,
	resolvers map[objectstorage.UploadTarget]DestinationIDResolver,
) ([]objectstorage.DestinationID, error) {
	if !cfg.Enabled() {
		return nil, nil
	}
	targets := cfg.Targets()
	if len(targets) == 0 {
		return nil, fmt.Errorf("object storage upload targets are required")
	}
	if err := objectstoragevalidation.ValidateUploadTargets(targets); err != nil {
		return nil, err
	}
	ids := make([]objectstorage.DestinationID, 0, len(targets))
	seen := make(map[objectstorage.DestinationID]struct{}, len(targets))
	for _, target := range targets {
		resolve, ok := resolvers[target]
		if !ok || resolve == nil {
			return nil, fmt.Errorf("object storage destination ID resolver is required for target %q", target)
		}
		resolved, err := resolve(cfg)
		if err != nil {
			return nil, fmt.Errorf("resolve %s object storage destination: %w", target, err)
		}
		if len(resolved) == 0 {
			return nil, fmt.Errorf("object storage target %q: %w", target, uploader.ErrDestinationIDRequired)
		}
		for _, id := range resolved {
			if strings.TrimSpace(string(id)) == "" {
				return nil, fmt.Errorf("object storage target %q: %w", target, uploader.ErrDestinationIDRequired)
			}
			if _, exists := seen[id]; exists {
				return nil, fmt.Errorf("object storage destination %q: %w", id, uploader.ErrDuplicateDestinationID)
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	return ids, nil
}
