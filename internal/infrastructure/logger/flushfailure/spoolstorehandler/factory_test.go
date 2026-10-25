package spoolstorehandler

import (
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/evacuation"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestNewOverloadHandlerProviderBuildsStdoutSpill(t *testing.T) {
	cfg := testSpoolStoreConfig{overload: testOverloadPolicyConfig{
		overloadPolicy:   pkgdurable.OverloadPolicyDegrade,
		spillDestination: pkgdurable.SpillDestinationStdout,
	}}

	handler, err := NewOverloadHandlerProvider(cfg, observer.NopMeter{})
	if err != nil {
		t.Fatalf("NewOverloadHandlerProvider error = %v", err)
	}
	if handler == nil {
		t.Fatal("NewOverloadHandlerProvider handler = nil")
	}
}

func TestNewOverloadHandlerProviderRejectsUnsupportedSpillDestinations(t *testing.T) {
	for _, destination := range []pkgdurable.SpillDestination{
		pkgdurable.SpillDestinationS3,
		pkgdurable.SpillDestinationNFS,
		pkgdurable.SpillDestination("unknown"),
	} {
		t.Run(string(destination), func(t *testing.T) {
			cfg := testSpoolStoreConfig{overload: testOverloadPolicyConfig{
				overloadPolicy:   pkgdurable.OverloadPolicyDegrade,
				spillDestination: destination,
			}}

			handler, err := NewOverloadHandlerProvider(cfg, observer.NopMeter{})
			if !errors.Is(err, evacuation.ErrUnsupportedSpillDestination) {
				t.Fatalf("error = %v, want ErrUnsupportedSpillDestination", err)
			}
			if handler != nil {
				t.Fatalf("handler = %T, want nil", handler)
			}
		})
	}
}

func TestNewOverloadHandlerProviderRejectsUnsupportedOverloadPolicy(t *testing.T) {
	cfg := testSpoolStoreConfig{overload: testOverloadPolicyConfig{
		overloadPolicy: pkgdurable.OverloadPolicy("future-policy"),
	}}

	handler, err := NewOverloadHandlerProvider(cfg, observer.NopMeter{})
	if !errors.Is(err, ErrUnsupportedSpoolOverloadPolicy) {
		t.Fatalf("error = %v, want ErrUnsupportedSpoolOverloadPolicy", err)
	}
	if handler != nil {
		t.Fatalf("handler = %T, want nil", handler)
	}
}

func TestNewOverloadHandlerProviderRejectsNegativeActiveBacklogWarnAfterDays(t *testing.T) {
	cfg := testSpoolStoreConfig{overload: testOverloadPolicyConfig{
		overloadPolicy:             pkgdurable.OverloadPolicyDrop,
		activeBacklogWarnAfterDays: -1,
	}}

	handler, err := NewOverloadHandlerProvider(cfg, observer.NopMeter{})
	if !errors.Is(err, ErrInvalidSpoolOverloadPolicyConfig) {
		t.Fatalf("error = %v, want ErrInvalidSpoolOverloadPolicyConfig", err)
	}
	if handler != nil {
		t.Fatalf("handler = %T, want nil", handler)
	}
}
