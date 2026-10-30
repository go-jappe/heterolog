package failurestore

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces/otlp"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/spool"
)

// replaySenderは、spoolのrecordをspanに戻し、送り先へ送り直します。spool/log/workerの
// Senderの実装です。
type replaySender struct {
	exporter traces.Exporter
	clock    clock.Clock
}

// Sendは、recordsのspanを1回のExportで送り、recordごとの結果を返します。spanに戻せない
// recordは、送らずにinvalid（quarantine）にします。送り先が送り直す扱いにしたspanは
// retryable、受信側が拒否したspanはpermanent（dead）、一部だけ受け付けられたspanは
// invalid（quarantine）です。
func (s replaySender) Send(ctx context.Context, records []spool.SpoolRecord) (spool.ExportReport, error) {
	report := make(spool.ExportReport, 0, len(records))
	spans := make([]tracing.SpanData, 0, len(records))
	indexes := make([]int, 0, len(records))
	for i, record := range records {
		decoded, err := otlp.UnmarshalJSON(record.Payload)
		if err == nil && len(decoded) != 1 {
			err = fmt.Errorf("%w: the record holds %d spans, want 1", otlp.ErrInvalidJSON, len(decoded))
		}
		if err != nil {
			report = append(report, spool.RecordOutcome{InputIndex: i, Status: spool.RecordOutcomeStatusInvalid, Err: err})
			continue
		}
		spans = append(spans, decoded[0])
		indexes = append(indexes, i)
	}
	if len(spans) == 0 {
		return report, nil
	}
	attemptedAt := s.clock.Now()
	outcomes := make([]spool.RecordOutcome, len(spans))
	for i := range outcomes {
		outcomes[i] = spool.RecordOutcome{InputIndex: indexes[i], Status: spool.RecordOutcomeStatusSuccess, AttemptedAt: attemptedAt}
	}
	if err := s.export(ctx, spans); err != nil {
		for _, failure := range traces.ExportFailures(err, len(spans)) {
			cause := failure.Err
			if cause == nil {
				cause = errors.New("the trace exporter failed")
			}
			for i := failure.Start; i < failure.End; i++ {
				outcomes[i].Err = cause
				switch failure.Disposition {
				case traces.DispositionDead:
					outcomes[i].Status = spool.RecordOutcomeStatusPermanent
				case traces.DispositionQuarantine:
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

// exportは、送り先のpanicを、すべてのspanを送り直す扱いの失敗にします。
func (s replaySender) export(ctx context.Context, spans []tracing.SpanData) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("trace exporter %s panicked: %v", s.exporter.Name(), r)
		}
	}()
	return s.exporter.Export(ctx, spans)
}
