package fsstore

import (
	"errors"
	"testing"
)

func TestValidateLogDirectory(t *testing.T) {
	for _, tc := range []struct {
		name      string
		directory string
		wantError bool
	}{
		{name: "empty", wantError: true},
		{name: "whitespace", directory: " \t\n", wantError: true},
		{name: "named directory", directory: "logs"},
		{name: "explicit target directory", directory: "."},
		{name: "nested directory", directory: "logs/application"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateLogDirectory(tc.directory)
			if tc.wantError {
				if !errors.Is(err, ErrEmptyLogDirectory) {
					t.Fatalf("ValidateLogDirectory(%q) = %v, want %v", tc.directory, err, ErrEmptyLogDirectory)
				}
			} else if err != nil {
				t.Fatalf("ValidateLogDirectory(%q) = %v", tc.directory, err)
			}
		})
	}
}

func TestValidateDebugLogDirectory(t *testing.T) {
	for _, tc := range []struct {
		name      string
		enabled   bool
		directory string
		wantError bool
	}{
		{name: "enabled empty", enabled: true, wantError: true},
		{name: "enabled whitespace", enabled: true, directory: " \t", wantError: true},
		{name: "enabled directory", enabled: true, directory: "debug"},
		{name: "disabled empty", directory: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateDebugLogDirectory(tc.enabled, tc.directory)
			if tc.wantError {
				if !errors.Is(err, ErrEmptyDebugLogDirectory) {
					t.Fatalf("ValidateDebugLogDirectory(%t, %q) = %v, want %v", tc.enabled, tc.directory, err, ErrEmptyDebugLogDirectory)
				}
			} else if err != nil {
				t.Fatalf("ValidateDebugLogDirectory(%t, %q) = %v", tc.enabled, tc.directory, err)
			}
		})
	}
}
