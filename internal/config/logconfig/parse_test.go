package logconfig

import (
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestParseLogCompressFormat(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		want      fsstore.CompressFormat
		wantError error
	}{
		{name: "gzip alias", value: " app.log.gz ", want: fsstore.CompressGzip},
		{name: "zstd alias", value: "Z-STANDARD", want: fsstore.CompressZstd},
		{name: "zstd extension", value: ".zst", want: fsstore.CompressZstd},
		{name: "lz4", value: "lz4", want: fsstore.CompressLz4},
		{name: "xz", value: "xz", want: fsstore.CompressXz},
		{name: "unknown is rejected", value: "brotli", wantError: ErrUnsupportedLogCompressFormat},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := ParseLogCompressFormat(testCase.value)
			if got != testCase.want {
				t.Fatalf("ParseLogCompressFormat(%q) = %q, want %q", testCase.value, got, testCase.want)
			}
			if testCase.wantError == nil {
				if err != nil {
					t.Fatalf("ParseLogCompressFormat(%q) error = %v, want nil", testCase.value, err)
				}
				return
			}
			if !errors.Is(err, testCase.wantError) {
				t.Fatalf("ParseLogCompressFormat(%q) error = %v, want %v", testCase.value, err, testCase.wantError)
			}
		})
	}
}

func TestParseLogFileLifecycleMode(t *testing.T) {
	tests := []struct {
		name         string
		value        string
		defaultMode  fsstore.FileLifecycleMode
		expectedMode fsstore.FileLifecycleMode
	}{
		{
			name:         "empty uses default",
			defaultMode:  fsstore.FileLifecycleModeRunner,
			expectedMode: fsstore.FileLifecycleModeRunner,
		},
		{
			name:         "inline",
			value:        " INLINE ",
			expectedMode: fsstore.FileLifecycleModeInline,
		},
		{
			name:         "runner",
			value:        "RUNNER",
			expectedMode: fsstore.FileLifecycleModeRunner,
		},
		{
			name:         "disable",
			value:        "disable",
			expectedMode: fsstore.FileLifecycleModeDisable,
		},
		{
			name:         "legacy logger remains invalid",
			value:        "logger",
			expectedMode: fsstore.FileLifecycleMode("logger"),
		},
		{
			name:         "legacy manager remains invalid",
			value:        "manager",
			expectedMode: fsstore.FileLifecycleMode("manager"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseLogFileLifecycleMode(tt.value, tt.defaultMode); got != tt.expectedMode {
				t.Fatalf(
					"ParseLogFileLifecycleMode(%q, %q) = %q, want %q",
					tt.value,
					tt.defaultMode,
					got,
					tt.expectedMode,
				)
			}
		})
	}
}
