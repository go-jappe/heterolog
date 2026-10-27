package victoriametrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewPushManagerRejectsNegativeSettings(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*PushManagerConfig)
	}{
		{"push interval", func(c *PushManagerConfig) { c.PushInterval = -time.Second }},
		{"batch size", func(c *PushManagerConfig) { c.BatchSize = -1 }},
		{"worker count", func(c *PushManagerConfig) { c.WorkerCount = -1 }},
		{"timeout", func(c *PushManagerConfig) { c.Timeout = -time.Second }},
		{"max retries", func(c *PushManagerConfig) { c.MaxRetries = -1 }},
		{"retry delay", func(c *PushManagerConfig) { c.RetryDelay = -time.Second }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Endpoint = "http://metrics.invalid"
			test.change(cfg)
			pm, err := NewPushManager(cfg)
			if pm != nil {
				defer pm.Stop()
				t.Fatal("negative setting constructed a manager")
			}
			if err == nil || !strings.Contains(err.Error(), test.name) {
				t.Fatalf("error = %v, want %s validation error", err, test.name)
			}
		})
	}
}

func TestNewPushManagerDefaultsCanDeliverSyncAndAsyncMetrics(t *testing.T) {
	for _, interval := range []time.Duration{0, 30 * time.Second} {
		t.Run(interval.String(), func(t *testing.T) {
			var mu sync.Mutex
			var bodies []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read request: %v", err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if len(body) == 0 {
					t.Error("valid manager configuration sent an empty batch")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				mu.Lock()
				bodies = append(bodies, string(body))
				mu.Unlock()
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			cfg := &PushManagerConfig{
				Endpoint: server.URL, PushInterval: interval, AppName: "test-app", Environment: "test",
			}
			pm, err := NewPushManager(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer pm.Stop()
			pm.collector = lifecycleMetricsCollector{}
			pm.AddCustomMetric("configured_metric", 7, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := pm.PushNow(ctx); err != nil {
				t.Fatalf("synchronous push with accepted configuration: %v", err)
			}
			if err := pm.PushAsync(); err != nil {
				t.Fatalf("asynchronous push with accepted configuration: %v", err)
			}
			// Startしていなくても、Stopは受け付けた非同期requestを処理し終えなければなりません。
			if err := pm.Stop(); err != nil {
				t.Fatalf("Stop before Start: %v", err)
			}
			if err := pm.Start(); err == nil {
				t.Fatal("Start accepted a manager whose client was already closed")
			}
			if cfg.BatchSize != 0 || cfg.WorkerCount != 0 || cfg.Timeout != 0 || cfg.PushInterval != interval || cfg.OnError != nil {
				t.Fatal("constructing the manager changed caller-owned settings")
			}
			if pm.config.PushInterval <= 0 || pm.config.Timeout <= 0 {
				t.Fatal("periodic pushes must receive a positive interval and timeout")
			}
			if pm.config.MaxRetries != 0 || pm.config.RetryDelay != 0 {
				t.Fatal("explicitly disabled retries or backoff acquired defaults")
			}
			mu.Lock()
			defer mu.Unlock()
			if len(bodies) != 2 {
				t.Fatalf("requests = %d, want one sync and one async push", len(bodies))
			}
			for _, body := range bodies {
				seen := make(map[string]int)
				for _, line := range strings.Split(strings.TrimSuffix(body, "\n"), "\n") {
					name, _, _ := strings.Cut(line, "{")
					seen[name]++
					if name == "configured_metric" && !strings.HasSuffix(line, "} 7") {
						t.Errorf("custom metric value changed: %s", line)
					}
				}
				for _, name := range []string{"configured_metric", "vm_push_manager_push_total", "vm_push_manager_error_total", "vm_push_manager_last_push_timestamp"} {
					if seen[name] != 1 {
						t.Errorf("metric %q count = %d, want 1", name, seen[name])
					}
					delete(seen, name)
				}
				if len(seen) != 0 {
					t.Errorf("unexpected metrics: %v", seen)
				}
			}
		})
	}
}
