package flushfailure

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestBuildSpoolHandlerRegistrarProvidesBorrowedDelegatingView(t *testing.T) {
	cfg := newSpoolOwnershipTestConfig(t)
	var registered []*spoolstorehandler.SpoolStoreHandler

	composite, err := Build(
		context.Background(),
		clock.NewUTCClock(),
		cfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		FactoryOptions{
			SpoolHandlerRegistrar: func(handler *spoolstorehandler.SpoolStoreHandler) error {
				registered = append(registered, handler)
				return nil
			},
		},
	)
	if err != nil {
		t.Fatalf("Build error = %v, want nil", err)
	}
	if len(registered) != 1 {
		t.Fatalf("registered spool handlers = %d, want 1", len(registered))
	}
	t.Cleanup(func() {
		_ = shutdownWithinHangTimeoutForTest(registered[0])
	})
	if len(composite.Handlers) != 1 {
		t.Fatalf("Composite handlers = %d, want 1", len(composite.Handlers))
	}

	borrowed := composite.Handlers[0]
	if _, ok := borrowed.(HandlerShutdown); ok {
		t.Fatalf("borrowed handler type %T implements HandlerShutdown", borrowed)
	}
	if err := borrowed.HandleFailure(context.Background(), loggerfailure.SendResult{}); err != nil {
		t.Fatalf("borrowed HandleFailure error = %v, want nil", err)
	}
	if err := drainWithinHangTimeoutForTest(composite); err != nil {
		t.Fatalf("Composite Drain error = %v, want nil", err)
	}
	if err := borrowed.HandleFailure(context.Background(), loggerfailure.SendResult{}); err != nil {
		t.Fatalf("borrowed HandleFailure after Composite Drain error = %v, want nil", err)
	}

	if err := shutdownWithinHangTimeoutForTest(registered[0]); err != nil {
		t.Fatalf("registered handler Shutdown error = %v, want nil", err)
	}
	if err := borrowed.HandleFailure(context.Background(), loggerfailure.SendResult{}); !errors.Is(err, spoolstorehandler.ErrSpoolStoreHandlerClosing) {
		t.Fatalf("borrowed HandleFailure after owner shutdown error = %v, want ErrSpoolStoreHandlerClosing", err)
	}
}

func TestBuildSpoolWithoutRegistrarRetainsDirectShutdownOwnership(t *testing.T) {
	cfg := newSpoolOwnershipTestConfig(t)

	composite, err := Build(
		context.Background(),
		clock.NewUTCClock(),
		cfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		FactoryOptions{},
	)
	if err != nil {
		t.Fatalf("Build error = %v, want nil", err)
	}
	if len(composite.Handlers) != 1 {
		t.Fatalf("Composite handlers = %d, want 1", len(composite.Handlers))
	}

	direct, ok := composite.Handlers[0].(*spoolstorehandler.SpoolStoreHandler)
	if !ok {
		t.Fatalf("Composite handler type = %T, want *spoolstorehandler.SpoolStoreHandler", composite.Handlers[0])
	}
	if _, ok := composite.Handlers[0].(HandlerShutdown); !ok {
		t.Fatalf("direct handler type %T does not implement HandlerShutdown", composite.Handlers[0])
	}
	t.Cleanup(func() {
		_ = shutdownWithinHangTimeoutForTest(direct)
	})

	if err := drainWithinHangTimeoutForTest(composite); err != nil {
		t.Fatalf("Composite Drain error = %v, want nil", err)
	}
	if err := direct.HandleFailure(context.Background(), loggerfailure.SendResult{}); !errors.Is(err, spoolstorehandler.ErrSpoolStoreHandlerClosing) {
		t.Fatalf("direct HandleFailure after Composite Drain error = %v, want ErrSpoolStoreHandlerClosing", err)
	}
}

func TestBuildRetainsRejectedSpoolHandlerForDrainRetry(t *testing.T) {
	cfg := newSpoolOwnershipTestConfig(t)
	rejection := errors.New("reject spool handler")
	var rejected *spoolstorehandler.SpoolStoreHandler

	composite, err := Build(
		context.Background(),
		clock.NewUTCClock(),
		cfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		FactoryOptions{
			SpoolHandlerRegistrar: func(handler *spoolstorehandler.SpoolStoreHandler) error {
				rejected = handler
				return rejection
			},
		},
	)
	if !errors.Is(err, rejection) {
		t.Fatalf("Build error = %v, want registrar rejection", err)
	}
	if rejected == nil {
		t.Fatal("registrar did not receive spool handler")
	}
	if len(composite.Handlers) != 1 {
		t.Fatalf("Composite handlers = %d, want 1", len(composite.Handlers))
	}
	if composite.Handlers[0] != rejected {
		t.Fatalf("Composite handler = %T, want rejected raw spool handler", composite.Handlers[0])
	}
	if err := rejected.HandleFailure(context.Background(), loggerfailure.SendResult{}); err != nil {
		t.Fatalf("rejected handler HandleFailure before Drain error = %v, want nil", err)
	}
	if err := drainWithinHangTimeoutForTest(composite); err != nil {
		t.Fatalf("Composite Drain error = %v, want nil", err)
	}
	if err := rejected.HandleFailure(context.Background(), loggerfailure.SendResult{}); !errors.Is(err, spoolstorehandler.ErrSpoolStoreHandlerClosing) {
		t.Fatalf("rejected handler HandleFailure after Drain error = %v, want ErrSpoolStoreHandlerClosing", err)
	}
}

func newSpoolOwnershipTestConfig(t *testing.T) loggerdurable.FailureStoreConfigAdapter {
	t.Helper()
	return loggerdurable.NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
		MaxConcurrent: 1,
		DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeSpool},
		DurableStoreConfig: pkgdurable.DurableStoreConfig{
			SpoolConfig: pkgdurable.SpoolConfig{
				SpoolPath: t.TempDir(),
				Target:    "test-app",
				Dir:       "test-backend",
				OverloadPolicyConfig: pkgdurable.SpoolOverloadPolicyConfig{
					OverloadPolicy: pkgdurable.OverloadPolicyDrop,
				},
			},
		},
	})
}
