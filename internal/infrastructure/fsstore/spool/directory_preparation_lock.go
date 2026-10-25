package spool

import (
	"context"
	"errors"
	"strings"
	"sync"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

type directoryPreparationLock struct {
	mu infrafsstore.ContextMutex
	// usersは取得待ちと取得済みの呼び出し数です。0になればregistryから除きます。
	// 親の同期失敗はinfrafsstoreがprocess全体で記録するため、ここでは保持しません。
	users int
}

// directoryPreparationRegistryは同じrootへの検査・隔離・再作成を直列化します。
// 初回退避用と再送用の異なるStoreでも共有し、異なるroot同士は待ち合わせません。
var directoryPreparationRegistry = struct {
	mu     sync.Mutex
	byRoot map[string]*directoryPreparationLock
}{byRoot: make(map[string]*directoryPreparationLock)}

// lockDirectoryPreparationはrootのlockを取得し、解放関数を返します。
func lockDirectoryPreparation(ctx context.Context, root string) (func(), error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("spool root must not be empty")
	}
	key, err := pathidentity.AbsoluteKey(root)
	if err != nil {
		return nil, err
	}
	directoryPreparationRegistry.mu.Lock()
	lock := directoryPreparationRegistry.byRoot[key]
	if lock == nil {
		lock = &directoryPreparationLock{}
		directoryPreparationRegistry.byRoot[key] = lock
	}
	lock.users++
	directoryPreparationRegistry.mu.Unlock()
	unregister := func() {
		directoryPreparationRegistry.mu.Lock()
		lock.users--
		if lock.users == 0 {
			delete(directoryPreparationRegistry.byRoot, key)
		}
		directoryPreparationRegistry.mu.Unlock()
	}
	if err := lock.mu.LockContext(ctx); err != nil {
		unregister()
		return nil, err
	}
	return func() {
		unregister()
		lock.mu.Unlock()
	}, nil
}
