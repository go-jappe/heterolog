package provider

import (
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/evacuation"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

type spillDestinationConfig struct {
	destination pkgdurable.SpillDestination
}

func (c spillDestinationConfig) SpillDestination() pkgdurable.SpillDestination {
	return c.destination
}

func TestNewDegradeHandlerBuildsStdoutOnly(t *testing.T) {
	handler, err := NewDegradeHandler(
		spillDestinationConfig{destination: pkgdurable.SpillDestinationStdout},
	)
	if err != nil {
		t.Fatalf("NewDegradeHandler error = %v", err)
	}
	if handler == nil {
		t.Fatal("handler = nil, want stdout evacuation handler")
	}
}

func TestNewDegradeHandlerRejectsUnsupportedDestinations(t *testing.T) {
	tests := []struct {
		name        string
		destination pkgdurable.SpillDestination
	}{
		{name: "s3", destination: pkgdurable.SpillDestinationS3},
		{name: "nfs", destination: pkgdurable.SpillDestinationNFS},
		{name: "unknown", destination: pkgdurable.SpillDestination("unknown")},
		{name: "empty", destination: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, err := NewDegradeHandler(
				spillDestinationConfig{destination: tt.destination},
			)
			if !errors.Is(err, evacuation.ErrUnsupportedSpillDestination) {
				t.Fatalf("error = %v, want ErrUnsupportedSpillDestination", err)
			}
			if handler != nil {
				t.Fatalf("handler = %T, want nil", handler)
			}
			var unsupported *evacuation.UnsupportedSpillDestinationError
			if !errors.As(err, &unsupported) || unsupported.Destination != tt.destination {
				t.Fatalf("error = %v, want UnsupportedSpillDestinationError for %q", err, tt.destination)
			}
		})
	}
}

func TestNewDegradeHandlerRejectsNilConfig(t *testing.T) {
	handler, err := NewDegradeHandler(nil)
	if !errors.Is(err, ErrNilSpillDestinationConfig) {
		t.Fatalf("error = %v, want ErrNilSpillDestinationConfig", err)
	}
	if handler != nil {
		t.Fatalf("handler = %T, want nil", handler)
	}
}
