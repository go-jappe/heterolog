package opensearch

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestBuildDeliveryFailureHandlerRejectsOpenSearchSpool(t *testing.T) {
	t.Parallel()

	fmCfg := NewDeliveryFlushManagerConfigAdapter(pkglogger.DeliveryOpenSearchFlushManagerConfig{
		Resource: pkglogger.Resource{Cloud: &pkglogger.CloudResourceInfo{}},
		FailureStoreConfig: pkgdurable.DeliveryFailureStoreConfig{
			DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeSpool},
		},
	})
	handler, err := BuildDeliveryFailureHandler(
		context.Background(),
		clock.NewUTCClock(),
		fmCfg,
		observer.NopTracer{},
		observer.NopMeter{},
		nil,
		nil,
	)
	var durableTypeErr *pkgdurable.DurableTypeError
	if !errors.As(err, &durableTypeErr) || *durableTypeErr != (pkgdurable.DurableTypeError{
		Scope: "delivery failure store", Type: pkgdurable.DurableTypeSpool, Err: pkgdurable.ErrUnsupportedDurableType,
	}) {
		t.Fatalf("BuildDeliveryFailureHandler() error = %v, want unsupported spool error", err)
	}
	if len(handler.Handlers) != 0 {
		t.Fatalf("handler count = %d, want 0", len(handler.Handlers))
	}
}
