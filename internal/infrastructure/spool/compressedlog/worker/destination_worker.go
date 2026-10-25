package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// 宛先別spoolを処理するWorkerを構築します
// workerは宛先別のspoolから一周期につき一件を再送します。
// 宛先を順番に選び、失敗中の宛先が他宛先の再送を止めないようにします。
// Retry-Afterの待機中の宛先はファイルを取得せずに飛ばし、同じ周期で次の宛先を処理します。
func NewDestinationWorker(stores *objectstoragespool.DestinationStores, routes RouteRegistry, cooldown time.Duration, options ...Option) (*Worker, error) {
	if stores == nil {
		return nil, ErrNilStore
	}
	w, err := newWorker(routes, cooldown, options...)
	if err != nil {
		return nil, err
	}
	w.destinationStores = stores
	return w, nil
}

// RecordRetryAfterは、spoolへ保存する前の通常のuploadが一時的な失敗とともに返したRetry-Afterを、
// rootの宛先の待機期限として記録し、宛先別spoolのmarkerへ保存して、記録後の期限を返します。
// このWorkerは次の周期から、期限までその宛先のファイルを送信しません。より後の期限を記録済みなら
// 短縮せず、上限を超える指定は上限までに短縮してRunReportで通知します。並行に呼び出せます。
func (w *Worker) RecordRetryAfter(root objectstoragespool.Root, retryAfter time.Duration) time.Time {
	if w == nil {
		return time.Time{}
	}
	return w.retryAfter.record(root, retryAfter)
}

func (w *Worker) routesForDestination(id objectstorage.DestinationID) RouteRegistry {
	routes := w.routes
	if w.routeSnapshot != nil {
		routes = w.routeSnapshot()
	}
	var selected RouteRegistry
	for _, route := range routes {
		if singleDestinationID(route.Uploader) == id {
			selected = append(selected, route)
		}
	}
	return selected
}

func (w *Worker) runDestinationOnce(ctx context.Context) (RunReport, error) {
	if err := ctx.Err(); err != nil {
		return RunReport{}, err
	}
	stores, scanErr := w.destinationStores.Existing(ctx)
	errs := []error{scanErr}
	var registered []*objectstoragespool.Store
	for _, store := range stores {
		id := store.Root().DestinationID()
		if len(w.routesForDestination(id)) != 0 {
			registered = append(registered, store)
			continue
		}
		// 未登録宛先はclaimも回復も行いません。未送信ファイルをそのまま保持します。
		for _, directory := range []string{store.Root().ActiveDir(), store.Root().ProcessingDir()} {
			if _, err := pathidentity.ResolveAndValidateManagedDirectory(store.Root().Path(), filepath.Base(directory)); err != nil {
				errs = append(errs, err)
				continue
			}
			entries, err := infrafsstore.ReadDirectoryEntries(directory)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if len(entries) > 0 {
				errs = append(errs, fmt.Errorf("%w: destination %q has pending files in %q", ErrNoMatchingRoute, id, directory))
			}
		}
	}
	start := 0
	for i, store := range registered {
		if store.Root().DestinationID() <= w.lastDestination {
			start = i + 1
		}
	}
	var report RunReport
	for offset := range registered {
		store := registered[(start+offset)%len(registered)]
		id := store.Root().DestinationID()
		// 再起動前に保存した待機期限を、この宛先へ最初に送信する前に読み込みます。
		w.retryAfter.restore(store.Root())
		// 送信先が指定した待機期限までは、この宛先のファイルを取得・回復せずに残します。
		if !w.retryAfter.deferredUntil(id).IsZero() {
			continue
		}
		child := &Worker{store: store, circuitBreakers: w.circuitBreakers, retryAfter: w.retryAfter, claimMu: w.claimMu,
			routeSnapshot: func() RouteRegistry { return w.routesForDestination(id) }}
		current, err := child.RunOnce(ctx)
		report.RecoveredProcessingCount += current.RecoveredProcessingCount
		report.RetryAfterMarkers = append(report.RetryAfterMarkers, current.RetryAfterMarkers...)
		if err != nil {
			errs = append(errs, fmt.Errorf("replay destination %q: %w", id, err))
		}
		if current.File.Claimed {
			report.File = current.File
			w.lastDestination = id
			break
		}
		if ctx.Err() != nil {
			break
		}
	}
	report.RetryAfterMarkers = append(report.RetryAfterMarkers, w.retryAfter.takeReports()...)
	return report, errors.Join(errs...)
}
