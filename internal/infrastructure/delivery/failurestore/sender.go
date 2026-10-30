package failurestore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/spool"
)

// replaySenderは、spoolのrecordを送るものへ戻し、送り先へ送り直します。spool/log/workerの
// Senderの実装です。
type replaySender[T any] struct {
	exporter delivery.Exporter[T]
	decode   func(payload []byte) (T, error)
	clock    clock.Clock
	// maxAgeは、送り直す期間の上限です。0なら上限はありません。
	maxAge time.Duration
}

// ErrTooOldは、送り直す期間の上限（Config.MaxAge）を過ぎたrecordを、送らずにdeadにした
// ことを表します。
var ErrTooOld = errors.New("the record is older than the maximum age for a resend")

// Sendは、recordsを1回のExportで送り、recordごとの結果を返します。戻せないrecordは、
// 送らずにinvalid（quarantine）にし、送り直す期間の上限を過ぎたrecordは、送らずに
// permanent（dead）にします。送り先が送り直す扱いにしたものはretryable、
// 受信側が拒否したものはpermanent（dead）、一部だけ受け付けられたものはinvalid
// （quarantine）です。
func (s replaySender[T]) Send(ctx context.Context, records []spool.SpoolRecord) (spool.ExportReport, error) {
	report := make(spool.ExportReport, 0, len(records))
	items := make([]T, 0, len(records))
	indexes := make([]int, 0, len(records))
	now := s.clock.Now()
	for i, record := range records {
		if s.maxAge > 0 && !record.OccurredAt.IsZero() && now.Sub(record.OccurredAt) > s.maxAge {
			report = append(report, spool.RecordOutcome{InputIndex: i, Status: spool.RecordOutcomeStatusPermanent,
				Err: fmt.Errorf("%w (failed at %s)", ErrTooOld, record.OccurredAt.Format(time.RFC3339))})
			continue
		}
		item, err := s.decode(record.Payload)
		if err != nil {
			report = append(report, spool.RecordOutcome{InputIndex: i, Status: spool.RecordOutcomeStatusInvalid, Err: err})
			continue
		}
		items = append(items, item)
		indexes = append(indexes, i)
	}
	if len(items) == 0 {
		return report, nil
	}
	attemptedAt := now
	outcomes := make([]spool.RecordOutcome, len(items))
	for i := range outcomes {
		outcomes[i] = spool.RecordOutcome{InputIndex: indexes[i], Status: spool.RecordOutcomeStatusSuccess, AttemptedAt: attemptedAt}
	}
	if err := s.export(ctx, items); err != nil {
		for _, failure := range delivery.ExportFailures(err, len(items)) {
			cause := failure.Err
			if cause == nil {
				cause = errors.New("the exporter failed")
			}
			for i := failure.Start; i < failure.End; i++ {
				outcomes[i].Err = cause
				switch failure.Disposition {
				case delivery.DispositionDead:
					outcomes[i].Status = spool.RecordOutcomeStatusPermanent
				case delivery.DispositionQuarantine:
					outcomes[i].Status = spool.RecordOutcomeStatusInvalid
				default:
					outcomes[i].Status = spool.RecordOutcomeStatusRetryable
					outcomes[i].RetryAfter = failure.RetryAfter
				}
			}
		}
	}
	return append(report, outcomes...), nil
}

// exportは、送り先のpanicを、すべてを送り直す扱いの失敗にします。
func (s replaySender[T]) export(ctx context.Context, items []T) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("trace exporter %s panicked: %v", s.exporter.Name(), r)
		}
	}()
	return s.exporter.Export(ctx, items)
}
