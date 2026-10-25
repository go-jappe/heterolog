package compressedlog

import (
	"errors"
	"fmt"
	"slices"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

var (
	ErrSpoolRootConfigMismatch  = errors.New("compressed log spool root has incompatible settings")
	ErrSpoolServiceRootMismatch = errors.New("compressed log spool service has multiple roots in one binding")
)

// RootRuntimeBuilderは一つのloggerの構築中だけ、保存先ごとにruntimeを共有します。
// 設定のポインタではなく、リンク解決済みrootの比較キーと固定した設定値を照合します。
// 同じrootで周期・保持設定が異なる場合はエラーにします。
// 構築後はOwnedRuntimesの一覧を所有者へ渡し、このbuilderを保持する必要はありません。
// 構築中の直列呼び出しを前提とし、登録済みruntimeを閉じた後は再利用しません。
type RootRuntimeBuilder struct {
	byRoot map[string]rootRuntimeBinding
	owned  []*RootRuntime
}

type rootRuntimeBinding struct {
	runtime *RootRuntime
	config  rootRuntimeConfigAdapter
}

func NewRootRuntimeBuilder() *RootRuntimeBuilder {
	return &RootRuntimeBuilder{byRoot: make(map[string]rootRuntimeBinding)}
}

// Buildは一つの利用先へ渡すbackend別runtimeを返します。
// 以前のBuildと同じ保存先・設定ならruntimeを共有します。同じbackendが今回の入力に
// 複数ある場合は保存先も同じである必要があります。失敗前に取得した所有権は保持するため、
// 構築を中止する呼び出し元はCloseを呼んでください。
func (b *RootRuntimeBuilder) Build(configs ...ConfigProvider) (RootRuntimes, error) {
	runtimes := make(RootRuntimes, len(configs))
	for _, config := range configs {
		root, fixedConfig, err := resolveRootRuntimeConfig(config)
		if err != nil {
			return nil, err
		}
		service := root.Service()
		rootKey := pathidentity.ComparisonKey(root.Path())
		if previous := runtimes[service]; previous != nil && pathidentity.ComparisonKey(previous.Root().Path()) != rootKey {
			return nil, fmt.Errorf("%w: %q uses %q and %q", ErrSpoolServiceRootMismatch, service, previous.Root().Path(), root.Path())
		}
		if binding, found := b.byRoot[rootKey]; found {
			if binding.config != fixedConfig {
				return nil, fmt.Errorf("%w: %q", ErrSpoolRootConfigMismatch, root.Path())
			}
			runtimes[service] = binding.runtime
			continue
		}
		runtime, err := newRootRuntime(root, fixedConfig)
		if err != nil {
			return nil, fmt.Errorf("create compressed log spool for %q: %w", service, err)
		}
		if b.byRoot == nil {
			b.byRoot = make(map[string]rootRuntimeBinding)
		}
		b.byRoot[rootKey] = rootRuntimeBinding{runtime: runtime, config: fixedConfig}
		b.owned = append(b.owned, runtime)
		runtimes[service] = runtime
	}
	return runtimes, nil
}

// OwnedRuntimesは構築側がCloseすべきruntimeを、重複のない一覧のコピーとして返します。
func (b *RootRuntimeBuilder) OwnedRuntimes() []*RootRuntime {
	return slices.Clone(b.owned)
}

// Closeは構築途中で取得した全保存先の所有権を解放します。
// 通常の終了では、所有者へ渡したOwnedRuntimesの各runtimeを閉じてください。
func (b *RootRuntimeBuilder) Close() error {
	var errs []error
	for _, runtime := range b.owned {
		errs = append(errs, runtime.Close())
	}
	return errors.Join(errs...)
}
