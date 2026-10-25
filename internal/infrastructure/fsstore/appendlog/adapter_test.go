package appendlog

import (
	"testing"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestFsstoreConfigAdapterResolvesDebugLogDirectory(t *testing.T) {
	for _, tt := range []struct {
		name        string
		separateDir bool
		debugDir    string
		want        string
	}{
		{name: "disabled shares normal directory", want: "logs"},
		{name: "disabled ignores stale directory", debugDir: "old-debug-logs", want: "logs"},
		{name: "enabled uses configured directory", separateDir: true, debugDir: "debug-logs", want: "debug-logs"},
		{name: "enabled preserves empty value for validation", separateDir: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			layout := NewFsstoreConfigAdapter(pkgfsstore.FsstoreConfig{
				LogDir:                       "logs",
				DebugLogDir:                  tt.debugDir,
				IsSeparateDebugLogDirEnabled: tt.separateDir,
			})
			if got := layout.DebugLogDir(); got != tt.want {
				t.Fatalf("DebugLogDir() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFilePurgeConfigAdaptersPreserveUsageCacheTTL(t *testing.T) {
	tests := []struct {
		name    string
		adapter FilePurgeConfigProvider
		want    time.Duration
	}{
		{
			name:    "normal zero",
			adapter: NewFilePurgeConfigAdapter(pkgfsstore.FileLifecycleConfig{}),
		},
		{
			name: "normal negative",
			adapter: NewFilePurgeConfigAdapter(pkgfsstore.FileLifecycleConfig{
				UsageCacheTTL: -time.Second,
			}),
			want: -time.Second,
		},
		{
			name: "delivery configured",
			adapter: NewDeliveryFilePurgeConfigAdapter(
				pkgfsstore.DeliveryFileLifecycleConfig{UsageCacheTTL: 23 * time.Minute},
			),
			want: 23 * time.Minute,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.adapter.UsageCacheTTL(); got != test.want {
				t.Fatalf("UsageCacheTTL() = %s, want %s", got, test.want)
			}
		})
	}
}
