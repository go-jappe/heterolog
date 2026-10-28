package degradehandler

import (
	"context"
	"testing"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestNewCountsDegradedRecords(t *testing.T) {
	meter := observertest.NewMeter()
	result := loggerfailure.SendResult{
		Meta:             loggerfailure.Meta{Backend: "cloudwatchlogs"},
		RetryableRecords: []loggerfailure.FailedRecord{{}, {}},
	}

	// 退避先のwriterは構築時のos.Stdoutを使うため、置き換えたstdoutの中で構築します。
	testutil.CaptureStdout(t, func() {
		handler, err := New(pkgdurable.SpillType(pkgdurable.SpillDestinationStdout), meter)
		if err != nil {
			t.Fatal(err)
		}
		if err := handler.HandleFailure(context.Background(), result); err != nil {
			t.Fatal(err)
		}
	})

	key := observertest.Key(telemetry.FailureRecords, telemetry.AttrBackend, "cloudwatchlogs", telemetry.AttrAction, telemetry.ActionDegraded)
	if got := meter.Count(key); got != 2 {
		t.Errorf("%s = %d, want 2; counters = %v", key, got, meter.Counters())
	}
}
