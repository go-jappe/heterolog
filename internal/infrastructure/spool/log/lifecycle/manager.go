package lifecycle

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle/report"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// ErrNilStoreは、構築時にlifecycle storeがnilであることを表します。
var ErrNilStore = errors.New("lifecycle store is nil")

// ErrPurgeFailedは、終了済みファイルの削除が失敗したことを表します。
// 実際に返すerrorはPurgeErrorで、失敗したstateを保持します。
var ErrPurgeFailed = errors.New("spool lifecycle purge failed")

// PurgeErrorは、stateごとの終了済みファイルの削除が失敗したことを表します。
// errors.Is(err, ErrPurgeFailed)で判定でき、原因のerrorもerrors.Is・errors.Asで辿れます。
type PurgeError struct {
	// Stateは、削除に失敗したstateです。
	State pkgspool.State
	// Errは、削除に失敗した原因です。
	Err error
}

func (e *PurgeError) Error() string { return fmt.Sprintf("purge %s: %v", e.State, e.Err) }

// Isは、ErrPurgeFailedと一致します。
func (e *PurgeError) Is(target error) bool { return target == ErrPurgeFailed }

// Unwrapは、削除に失敗した原因を返します。
func (e *PurgeError) Unwrap() error { return e.Err }

type Manager struct {
	Store Store

	// purge結果とエラーはRunOnceから返します。Loggerへの内部通知はrunnerの
	// Reporterが担当するため、ここでは個別に通知しません。
	Tracer pkgobserver.Tracer
	Meter  pkgobserver.Meter
}

func NewManager(
	store Store,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (*Manager, error) {
	if store == nil {
		return nil, ErrNilStore
	}

	return &Manager{
		Store:  store,
		Tracer: tracer,
		Meter:  meter,
	}, nil
}

func (m *Manager) RunOnce(ctx context.Context) (report.RunReport, error) {
	var report report.RunReport
	var errs []error

	var err error

	report.DonePurgeReport, err = m.Store.PurgeDoneIfNeed(ctx)
	if err != nil {
		if isContextError(err) {
			return report, &PurgeError{State: pkgspool.StateDone, Err: err}
		}
		errs = append(errs, &PurgeError{State: pkgspool.StateDone, Err: err})
	}

	report.DeadPurgeReport, err = m.Store.PurgeDeadIfNeed(ctx)
	if err != nil {
		if isContextError(err) {
			return report, &PurgeError{State: pkgspool.StateDead, Err: err}
		}
		errs = append(errs, &PurgeError{State: pkgspool.StateDead, Err: err})
	}

	report.QuarantinePurgeReport, err = m.Store.PurgeQuarantineIfNeed(ctx)
	if err != nil {
		if isContextError(err) {
			return report, &PurgeError{State: pkgspool.StateQuarantine, Err: err}
		}
		errs = append(errs, &PurgeError{State: pkgspool.StateQuarantine, Err: err})
	}

	return report, errors.Join(errs...)
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}
