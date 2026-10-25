package fsstore

import (
	"errors"
	"testing"
)

func TestValidateFileLifecycleMode(t *testing.T) {
	tests := []struct {
		name    string
		mode    FileLifecycleMode
		wantErr bool
	}{
		{name: "empty uses inline compatibility", mode: ""},
		{name: "disable", mode: FileLifecycleModeDisable},
		{name: "inline", mode: FileLifecycleModeInline},
		{name: "runner", mode: FileLifecycleModeRunner},
		{name: "legacy logger", mode: FileLifecycleMode("logger"), wantErr: true},
		{name: "legacy manager", mode: FileLifecycleMode("manager"), wantErr: true},
		{name: "unknown", mode: FileLifecycleMode("unknown"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateFileLifecycleMode(tt.mode)
			if (err != nil) != tt.wantErr || (tt.wantErr && !errors.Is(err, ErrUnsupportedFileLifecycleMode)) {
				t.Fatalf("ValidateFileLifecycleMode(%q) error = %v, wantErr %v", tt.mode, err, tt.wantErr)
			}
		})
	}
}

func TestValidateDeliveryFileLifecycleMode(t *testing.T) {
	tests := []struct {
		name    string
		mode    FileLifecycleMode
		wantErr bool
	}{
		{name: "disable", mode: FileLifecycleModeDisable},
		{name: "runner", mode: FileLifecycleModeRunner},
		{name: "empty direct config compatibility", mode: ""},
		{name: "inline", mode: FileLifecycleModeInline, wantErr: true},
		{name: "legacy logger", mode: FileLifecycleMode("logger"), wantErr: true},
		{name: "legacy manager", mode: FileLifecycleMode("manager"), wantErr: true},
		{name: "unknown", mode: FileLifecycleMode("unknown"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDeliveryFileLifecycleMode(tt.mode)
			if (err != nil) != tt.wantErr || (tt.wantErr && !errors.Is(err, ErrUnsupportedDeliveryFileLifecycleMode)) {
				t.Fatalf(
					"ValidateDeliveryFileLifecycleMode(%q) error = %v, wantErr %v",
					tt.mode,
					err,
					tt.wantErr,
				)
			}
		})
	}
}
