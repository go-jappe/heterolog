package filelifecycle

import (
	"testing"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestLifecycleResponsibilityPolicies(t *testing.T) {
	tests := []struct {
		name string
		mode pkgfsstore.FileLifecycleMode
		want bool
	}{
		{name: "unresolved empty", mode: "", want: false},
		{name: "inline", mode: pkgfsstore.FileLifecycleModeInline, want: true},
		{name: "runner", mode: pkgfsstore.FileLifecycleModeRunner, want: true},
		{name: "disable", mode: pkgfsstore.FileLifecycleModeDisable, want: false},
		{name: "unknown", mode: pkgfsstore.FileLifecycleMode("unknown"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := UsesUsageCache(tt.mode); got != tt.want {
				t.Fatalf("UsesUsageCache(%q) = %t, want %t", tt.mode, got, tt.want)
			}
			if got := UsesObjectStorageUpload(tt.mode); got != tt.want {
				t.Fatalf("UsesObjectStorageUpload(%q) = %t, want %t", tt.mode, got, tt.want)
			}
		})
	}
}
