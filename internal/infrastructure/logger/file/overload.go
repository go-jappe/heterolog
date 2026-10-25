package file

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/evacuation"
	"github.com/go-jappe/heterolog/pkg/disk"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// ErrFileStorageOverloadedは、空き容量不足のためfile書込みをdrop/退避したことを
// OnErrorへ通知します。退避に成功した場合、ログ呼出し自体はnilを返します。
// dropした場合は、記録しなかったことを判別できるようpkglogger.ErrLogDroppedを包んだerrorを返します。
var ErrFileStorageOverloaded = errors.New("file storage has insufficient available disk space")

// fileOverloadは、通常ログの容量判定と、構築時に選択したdrop/退避処理を保持します。
// 判定対象はfilesystemの空き容量です。purgeのdirectory使用量cacheとは共有しません。
type fileOverload struct {
	config       FileOverloadConfigProvider
	freeAndTotal func(string) (uint64, uint64, error)
	handle       func(context.Context, string) error
}

func newFileOverload(cfg FileOverloadConfigProvider, meter observer.Meter) (*fileOverload, error) {
	if cfg == nil {
		return nil, nil
	}
	if err := pkgfsstore.ValidateFileOverloadConfig(cfg.OverloadPolicy(), cfg.SpillDestination()); err != nil {
		return nil, err
	}
	overload := &fileOverload{config: cfg, freeAndTotal: disk.FreeAndTotal}
	if cfg.OverloadPolicy() == pkgfsstore.OverloadPolicyDegrade {
		overload.handle = evacuation.WriteTextToStdout
	} else {
		overload.handle = func(ctx context.Context, _ string) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			meter.Counter("file_overload_drop_total").Add(ctx, 1)
			if span := observer.SpanFromContext(ctx); span != nil {
				span.AddEvent("file_overload_drop")
			}
			// 破棄したことを同期APIの呼び出し元がerrors.Isで判別できるよう、
			// ErrLogDroppedを包んだerrorを返します。
			return fmt.Errorf("%w: file storage is overloaded", pkglogger.ErrLogDropped)
		}
	}
	return overload, nil
}

// checkは書込み前に実保存先の容量を確認します。0なら容量制限を無効にします。
// 取得失敗を容量不足と決めつけてdropせず、呼び出し元へそのままエラーを渡します。
func (o *fileOverload) check(directory string) error {
	if o == nil || o.config.MinAvailableDiskSpace() == 0 {
		return nil
	}
	free, _, err := o.freeAndTotal(directory)
	if err != nil {
		return fmt.Errorf("inspect available disk space for file logger directory %q: %w", directory, err)
	}
	minimum := o.config.MinAvailableDiskSpace()
	if free < minimum {
		return fmt.Errorf("%w: directory=%q available=%d minimum=%d", ErrFileStorageOverloaded, directory, free, minimum)
	}
	return nil
}
