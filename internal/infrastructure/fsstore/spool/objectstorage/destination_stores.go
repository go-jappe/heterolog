package objectstoragespool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// DestinationStoresは圧縮ログのspoolを宛先IDごとに分けて管理します。
// service/logger契約の共通ルート直下に宛先別ディレクトリを置き、各宛先のStoreが
// 保存・状態遷移・削除を担当します。同名ファイルを別の宛先と取り違えません。
// upload自体は担当せず、再送workerが各Storeのファイルを取得して送信します。
//
// ディレクトリ構成:
//
//	共通ルート/
//	├─ 宛先ID-A/
//	│  ├─ active/
//	│  ├─ processing/
//	│  ├─ done/
//	│  ├─ dead/
//	│  ├─ quarantine/
//	│  └─ .preserved/（不正名・矛盾などの保全先。自動再送・purge対象外）
//	└─ 宛先ID-B/
//	   └─ 同じ状態別ディレクトリ
type DestinationStores struct {
	root   Root
	mu     sync.Mutex
	stores map[objectstorage.DestinationID]*Store
}

func NewDestinationStores(root Root) (*DestinationStores, error) {
	if err := root.validate(); err != nil {
		return nil, err
	}
	if root.DestinationID() != "" {
		return nil, fmt.Errorf("%w: expected service root", ErrInvalidRoot)
	}
	return &DestinationStores{root: root, stores: make(map[objectstorage.DestinationID]*Store)}, nil
}

func (s *DestinationStores) Root() Root { return s.root }

// EnsureLayoutは共通ルートだけを用意します。宛先配下は登録時に用意します。
func (s *DestinationStores) EnsureLayout(ctx context.Context) error {
	return infrafsstore.EnsureDirectoryNoSymlinksContext(ctx, s.root.Path(), directoryMode)
}

// ForDestinationは指定した宛先IDのStoreを返します。
// このDestinationStores内では同じ宛先に同じインスタンスを返し、ファイル操作の排他を共有します。
func (s *DestinationStores) ForDestination(id objectstorage.DestinationID) (*Store, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.stores[id]; existing != nil {
		return existing, nil
	}
	root, err := s.root.ForDestination(id)
	if err != nil {
		return nil, err
	}
	store, err := NewStore(root)
	if err != nil {
		return nil, err
	}
	s.stores[id] = store
	return store, nil
}

// Existingはディスクに残る宛先ディレクトリに対応するStoreを列挙します。
// 現在の送信設定にない宛先も含みます。列挙自体は送信可否の判定やuploadを行いません。
// 不正な名前やリンクは通知用のエラーにし、他の通常ディレクトリの列挙は続けます。
func (s *DestinationStores) Existing(ctx context.Context) ([]*Store, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	entries, err := infrafsstore.ReadDirectoryEntries(s.root.Path())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var stores []*Store
	var errs []error
	for _, entry := range entries {
		if err := contextError(ctx); err != nil {
			return stores, errors.Join(append(errs, err)...)
		}
		info, err := entry.Info()
		if err == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
			err = errors.New("destination spool entry is not a real directory")
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("inspect destination spool %q: %w", entry.Name(), err))
			continue
		}
		// ResolveAndValidateManagedDirectoryは、Windowsのjunction/reparse pointも拒否します。
		if _, err := s.root.ForDestination(objectstorage.DestinationID(entry.Name())); err != nil {
			errs = append(errs, err)
			continue
		}
		store, err := s.ForDestination(objectstorage.DestinationID(entry.Name()))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		stores = append(stores, store)
	}
	return stores, errors.Join(errs...)
}

// ListFilesはディスクに残る全宛先から、指定したstateのファイルを集めます。
// 共通ルート全体の容量purgeに使い、容量上限は宛先別ではなく全宛先の合計に適用します。
func (s *DestinationStores) ListFiles(ctx context.Context, state pkgspool.State) ([]CompressedLogFile, error) {
	stores, scanErr := s.Existing(ctx)
	var files []CompressedLogFile
	errs := []error{scanErr}
	for _, store := range stores {
		found, err := store.ListFiles(ctx, state)
		files = append(files, found...)
		if err != nil {
			errs = append(errs, err)
		}
	}
	return files, errors.Join(errs...)
}

// TakePreservationWarningsは、走査中に不正なエントリを.preservedへ移した警告を
// 全宛先から一度だけ取り出します。メモリ上のStore一覧を使い、directoryは走査しません。
// 戻った時点でStoreのlockは解放済みです。呼び出し元は警告をOnWarnへ通知してください。
func (s *DestinationStores) TakePreservationWarnings(ctx context.Context) ([]error, error) {
	s.mu.Lock()
	stores := make([]*Store, 0, len(s.stores))
	for _, store := range s.stores {
		stores = append(stores, store)
	}
	s.mu.Unlock()
	var warnings, errs []error
	for _, store := range stores {
		found, err := store.takePreservationWarnings(ctx)
		warnings = append(warnings, found...)
		if err != nil {
			errs = append(errs, err)
		}
	}
	return warnings, errors.Join(errs...)
}

// RemoveTerminalはfileの宛先IDから対応するStoreを選び、終了済みファイルの削除を委譲します。
func (s *DestinationStores) RemoveTerminal(ctx context.Context, file CompressedLogFile) error {
	store, err := s.ForDestination(file.DestinationID)
	if err != nil {
		return err
	}
	return store.RemoveTerminal(ctx, file)
}
