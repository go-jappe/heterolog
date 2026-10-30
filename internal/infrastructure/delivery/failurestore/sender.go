package failurestore

import (
	"context"
	"errors"
	"fmt"

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
}

// Sendは、recordsを1回のExportで送り、recordごとの結果を返します。戻せないrecordは、
// 送らずにinvalid（quarantine）にします。送り先が送り直す扱いにしたものはretryable、
// 受信側が拒否したものはpermanent（dead）、一部だけ受け付けられたものはinvalid
// （quarantine）です。
func (s replaySender[T]) Send(ctx context.Context, records []spool.SpoolRecord) (spool.ExportReport, error) {
	report := make(spool.ExportReport, 0, len(records))
	items := make([]T, 0, len(records))
	indexes := make([]int, 0, len(records))
	for i, record := range records {
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
	attemptedAt := s.clock.Now()
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
