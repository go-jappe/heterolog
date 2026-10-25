package compressedlog

import (
	"errors"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// RootRuntimesは、一つの利用先へ渡すbackend別の共有runtimeです。
// 同じbackendの宛先はruntimeを共有し、保存先はさらに宛先IDで分離します。
type RootRuntimes map[objectstorage.UploadTarget]*RootRuntime

// NewRootRuntimesは、構築側がbackendごとに用意した設定から共有runtimeを作ります。
// 同じbackendが複数回指定された場合、保存先と周期・保持設定の一致を確認します。
// 途中で構築に失敗した場合は、それまでに取得した保存先の所有権を解放します。
func NewRootRuntimes(configs ...ConfigProvider) (RootRuntimes, error) {
	builder := NewRootRuntimeBuilder()
	runtimes, err := builder.Build(configs...)
	if err != nil {
		return nil, errors.Join(err, builder.Close())
	}
	return runtimes, nil
}

// Closeは構築側が持つ各runtimeの利用登録を解除します。
// 登録済みrouteが残っていれば、そのrouteが閉じるまで共有処理は継続します。
func (r RootRuntimes) Close() error {
	var errs []error
	for _, runtime := range r {
		if err := runtime.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
