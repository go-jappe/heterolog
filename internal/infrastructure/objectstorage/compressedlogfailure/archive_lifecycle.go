package compressedlogfailure

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

var errArchiveLifecycleStopped = errors.New(
	"compressed log file upload failure archive lifecycle is stopped",
)

// ArchivePurgeObserverは、一回のarchiveのpurge結果とエラーを利用側へ通知します。
// 正常終了時もerr=nilで通知し、結果の記録方法は利用側が決めます。
// ロック待ちの中止など、purgeを開始できなかった場合のreportは空です。
// 通知も終了待ちに含むため、通知中に同じ所有者のClose完了を待たないでください。
type ArchivePurgeObserver func(ctx context.Context, report compressedlogarchive.PurgeReport, err error)

// errorログ、debugログのディレクトリが分離されていればOwnerが分離される
type archiveLifecycleOwner struct {
	coordinator *sharedArchiveLifecycle

	closeOnce sync.Once
	closeErr  error
	// callbacksは周期開始時にこのownerへ受け付けた結果通知です。
	callbacks   sync.WaitGroup
	forceCtx    context.Context
	forceCancel context.CancelFunc
	forced      bool // coordinator.muで保護します。
}

type sharedArchiveLifecycle struct {
	store  *compressedlogarchive.Store
	purger *compressedlogarchive.Purger
	// intervalはpurgeの実行周期です。保持設定はpurgerだけが保持します。
	interval time.Duration
	// 取得時に固定します。手動復旧と周期処理のownerは同じrootを共有できません。
	maintenanceDisabled bool
	// storeOrPurgeMuは保存とpurgeで共有するロックです。保存側がread lock、
	// purge側がwrite lockをそれぞれ取得し、両処理の同時実行を防ぎます。
	// 保存結果を呼び出し元へ返す前に、保存したファイルがpurgeされることを防ぎます。
	storeOrPurgeMu infrafsstore.ContextRWMutex

	mu             sync.Mutex
	purgeObservers map[*archiveLifecycleOwner]ArchivePurgeObserver
	// shutdownStartedは、最後のownerのcloseでtrueにし、新しい保存とownerの参加を
	// 受け付けなくするフラグです。保存やpurgeがまだ動いていてもtrueになります。
	// 終了完了の通知ではありません。すべての終了はshutdownCompleteで通知します。
	shutdownStarted bool
	// forceShutdownStartedは共有処理の強制中止後、新しいownerの参加を防ぎます。
	forceShutdownStarted bool

	// cancelは次のpurge周期の受付だけを止めます。受理済み処理は中止しません。
	cancel context.CancelFunc
	// operationContextは受理済みpurge用です。強制終了だけで中止します。
	operationContext context.Context
	forceCancel      context.CancelFunc
	// purgeLoopStoppedは、runがpurgeの周期処理を終了すると閉じる通知です。
	// 最後のownerのcloseが停止完了を待つために使います。保存処理の終了は含みません。
	purgeLoopStopped chan struct{}
	// shutdownCompleteは、最後のownerのcloseが、受理済みの保存とpurge周期処理の
	// 終了を待ち、この管理処理をregistryから削除した後に閉じる通知です。
	// 同じ保存先を使う新しいownerは、古い管理処理と重ならないよう、この通知を待ちます。
	shutdownComplete chan struct{}
}

var archiveLifecycleRegistry = struct {
	sync.Mutex
	byRoot map[string]*sharedArchiveLifecycle
}{byRoot: make(map[string]*sharedArchiveLifecycle)}

// acquireArchiveLifecycleは、同じarchive保存先のStoreとpurge周期処理を共有します。
// 同じ保存先には設定組立側で共通のcfgを配布するため、登録時の設定比較は行いません。
func acquireArchiveLifecycle(
	store *compressedlogarchive.Store,
	cfg FileArchiveConfigProvider,
	timeSource clock.Clock,
	observer ArchivePurgeObserver,
) (*archiveLifecycleOwner, *sharedArchiveLifecycle, error) {
	return acquireArchiveLifecycleWithMaintenance(store, cfg, timeSource, observer, false)
}

func acquireArchiveLifecycleWithMaintenance(
	store *compressedlogarchive.Store,
	cfg FileArchiveConfigProvider,
	timeSource clock.Clock,
	observer ArchivePurgeObserver,
	disableMaintenance bool,
) (*archiveLifecycleOwner, *sharedArchiveLifecycle, error) {
	if store == nil {
		return nil, nil, fmt.Errorf("%w: archive store is nil", ErrInvalidConfig)
	}
	lifecycleInterval := cfg.LifecycleInterval()
	if lifecycleInterval <= 0 {
		return nil, nil, fmt.Errorf("%w: archive lifecycle interval must be positive", ErrInvalidConfig)
	}
	purgeConfig := compressedlogarchive.NewPurgeConfigAdapter(compressedlogarchive.PurgeConfig{
		Retryable: cfg.RetryablePurge(),
		Permanent: cfg.PermanentPurge(),
	})
	// 共有済みでも入力は検証します。Purger自体は新しい管理処理にだけ作成します。
	if err := compressedlogarchive.ValidatePurgeConfig(purgeConfig); err != nil {
		return nil, nil, err
	}

	rootKey := pathidentity.ComparisonKey(store.Root().Path())
	for {
		archiveLifecycleRegistry.Lock()
		coordinator := archiveLifecycleRegistry.byRoot[rootKey]
		if coordinator != nil {
			coordinator.mu.Lock()
			if coordinator.shutdownStarted || coordinator.forceShutdownStarted {
				shutdownComplete := coordinator.shutdownComplete
				coordinator.mu.Unlock()
				archiveLifecycleRegistry.Unlock()
				// 古い管理処理の保存・purge・登録解除がすべて終わるまで待ちます。
				// 待機中はregistryのlockを解放し、完了後に登録状況を調べ直します。
				<-shutdownComplete
				continue
			}
			if coordinator.maintenanceDisabled != disableMaintenance {
				coordinator.mu.Unlock()
				archiveLifecycleRegistry.Unlock()
				return nil, nil, fmt.Errorf("%w: archive root %q has an incompatible maintenance mode", ErrInvalidConfig, rootKey)
			}
			owner := newArchiveLifecycleOwner(coordinator)
			coordinator.purgeObservers[owner] = observer
			coordinator.mu.Unlock()
			archiveLifecycleRegistry.Unlock()
			return owner, coordinator, nil
		}

		// 既存coordinatorへの参加では、そのStoreとlifecycleを共有します。
		// 新たに作る場合は、以前のownerが完全に終了してから保存先directoryを初期化し、
		// 正規化したrootを新coordinatorへ渡すまでregistry lockを保持します。
		purger, err := compressedlogarchive.NewPurger(store, purgeConfig, timeSource)
		if err != nil {
			archiveLifecycleRegistry.Unlock()
			return nil, nil, err
		}
		if err := store.EnsureDirectories(context.Background()); err != nil {
			archiveLifecycleRegistry.Unlock()
			return nil, nil, fmt.Errorf("initialize object storage compressed log file upload failure archive: %w", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		operationContext, forceCancel := context.WithCancel(context.Background())
		coordinator = &sharedArchiveLifecycle{
			store:               store,
			purger:              purger,
			interval:            lifecycleInterval,
			maintenanceDisabled: disableMaintenance,
			purgeObservers:      make(map[*archiveLifecycleOwner]ArchivePurgeObserver),
			cancel:              cancel,
			operationContext:    operationContext,
			forceCancel:         forceCancel,
			purgeLoopStopped:    make(chan struct{}),
			shutdownComplete:    make(chan struct{}),
		}
		owner := newArchiveLifecycleOwner(coordinator)
		coordinator.purgeObservers[owner] = observer
		archiveLifecycleRegistry.byRoot[rootKey] = coordinator
		archiveLifecycleRegistry.Unlock()
		if disableMaintenance {
			close(coordinator.purgeLoopStopped)
		} else {
			go coordinator.run(ctx)
		}
		return owner, coordinator, nil
	}
}

func newArchiveLifecycleOwner(coordinator *sharedArchiveLifecycle) *archiveLifecycleOwner {
	ctx, cancel := context.WithCancel(context.Background())
	return &archiveLifecycleOwner{coordinator: coordinator, forceCtx: ctx, forceCancel: cancel}
}

// requestForceCloseはこのownerの通知を中止可能にします。他のownerが残る間は
// 共有purgeを中止しません。すべてのownerが強制終了した場合だけ共有処理も止めます。
func (o *archiveLifecycleOwner) requestForceClose() {
	if o == nil || o.coordinator == nil {
		return
	}
	c := o.coordinator
	c.mu.Lock()
	o.forced = true
	if o.forceCancel != nil {
		o.forceCancel()
	}
	allForced := true
	for owner := range c.purgeObservers {
		if !owner.forced {
			allForced = false
			break
		}
	}
	if allForced && c.forceCancel != nil {
		c.forceShutdownStarted = true
		c.forceCancel()
		c.cancel()
	}
	c.mu.Unlock()
}

func (o *archiveLifecycleOwner) close() error {
	if o == nil || o.coordinator == nil {
		return nil
	}
	o.closeOnce.Do(func() {
		coordinator := o.coordinator
		rootKey := pathidentity.ComparisonKey(coordinator.store.Root().Path())
		archiveLifecycleRegistry.Lock()
		coordinator.mu.Lock()
		delete(coordinator.purgeObservers, o)
		lastOwner := len(coordinator.purgeObservers) == 0 && !coordinator.shutdownStarted
		if lastOwner {
			coordinator.shutdownStarted = true
			coordinator.cancel()
			if o.forced && coordinator.forceCancel != nil {
				coordinator.forceCancel()
			}
		}
		coordinator.mu.Unlock()
		archiveLifecycleRegistry.Unlock()
		// 削除前に受け付けた通知は、他のownerが残る場合も最後まで待ちます。
		o.callbacks.Wait()
		if lastOwner {
			// 停止中の登録を残して新しいownerを待たせ、受理済みの保存の完了を待ちます。
			// 保存とpurgeの両方が終わるまで、同じ保存先に新しい管理処理を作らせません。
			coordinator.storeOrPurgeMu.Lock()
			coordinator.storeOrPurgeMu.Unlock()
			<-coordinator.purgeLoopStopped
			archiveLifecycleRegistry.Lock()
			if archiveLifecycleRegistry.byRoot[rootKey] == coordinator {
				delete(archiveLifecycleRegistry.byRoot, rootKey)
			}
			close(coordinator.shutdownComplete)
			archiveLifecycleRegistry.Unlock()
		}
	})
	return o.closeErr
}

func (c *sharedArchiveLifecycle) run(ctx context.Context) {
	defer close(c.purgeLoopStopped)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	operationContext := c.operationContext
	if operationContext == nil {
		operationContext = ctx
	}
	if ctx.Err() != nil {
		return
	}
	c.runOnce(operationContext)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			c.runOnce(operationContext)
		}
	}
}

// runOnceは通常archiveのpurge結果に、.preservedの残存件数を管理者確認の通知として加えます。
// 件数はpurge側がfilesystemから取得するため、再起動後も通知します。
// 保全ファイルを削除・再送する処理ではありません。
func (c *sharedArchiveLifecycle) runOnce(ctx context.Context) {
	// 結果が出る前にownerがCloseしても通知を失わないよう、周期の入口で受け付けます。
	observers := c.acceptPurgeObservers()
	var report compressedlogarchive.PurgeReport
	err := c.storeOrPurgeMu.LockContext(ctx)
	if err == nil {
		report, err = runArchivePurgeSafely(ctx, c.purger)
		c.storeOrPurgeMu.Unlock()
	}
	if report.PreservedFileCount != 0 {
		err = errors.Join(err, &PreservedArchiveFilesError{Root: c.store.Root().Path(), Count: report.PreservedFileCount})
	}
	for _, observer := range observers {
		func() {
			defer observer.owner.callbacks.Done()
			defer func() { _ = recover() }()
			notificationContext := observer.owner.forceCtx
			if notificationContext == nil {
				notificationContext = ctx
			}
			observer.notify(notificationContext, report, err)
		}()
	}
}

// lockPurgeDuringSaveは保存中のpurgeを抑止するロックを取得し、解除用の関数を返します。
func (c *sharedArchiveLifecycle) lockPurgeDuringSave(ctx context.Context) (func(), error) {
	if c == nil {
		return func() {}, errArchiveLifecycleStopped
	}
	if err := c.storeOrPurgeMu.RLockContext(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	shutdownStarted := c.shutdownStarted || c.forceShutdownStarted
	c.mu.Unlock()
	if shutdownStarted {
		c.storeOrPurgeMu.RUnlock()
		return func() {}, errArchiveLifecycleStopped
	}
	return c.storeOrPurgeMu.RUnlock, nil
}

type acceptedArchivePurgeObserver struct {
	owner  *archiveLifecycleOwner
	notify ArchivePurgeObserver
}

// acceptPurgeObserversは、この周期の通知先を確定し、各ownerの終了待ちへ登録します。
// 通知はlock外で実行し、正常終了では通知用contextを中止しません。
func (c *sharedArchiveLifecycle) acceptPurgeObservers() []acceptedArchivePurgeObserver {
	c.mu.Lock()
	defer c.mu.Unlock()
	observers := make([]acceptedArchivePurgeObserver, 0, len(c.purgeObservers))
	for owner, observer := range c.purgeObservers {
		if observer != nil {
			owner.callbacks.Add(1)
			observers = append(observers, acceptedArchivePurgeObserver{owner: owner, notify: observer})
		}
	}
	return observers
}

func runArchivePurgeSafely(
	ctx context.Context,
	purger *compressedlogarchive.Purger,
) (report compressedlogarchive.PurgeReport, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &PanicError{Kind: ErrArchivePurgePanic, Value: recovered}
		}
	}()
	return purger.Run(ctx)
}
