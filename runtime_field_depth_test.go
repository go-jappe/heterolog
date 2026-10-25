package heterolog_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// nestedRuntimeValue wraps leaf in depth nested arrays.
func nestedRuntimeValue(depth int, leaf string) any {
	var value any = leaf
	for range depth {
		value = []any{value}
	}
	return value
}

// requireNestedRuntimeLeaf unwraps depth single-element arrays and checks leaf.
func requireNestedRuntimeLeaf(t *testing.T, value any, depth int, leaf string) {
	t.Helper()
	for level := range depth {
		array, ok := value.([]any)
		if !ok || len(array) != 1 {
			t.Fatalf("nesting lost at level %d: value type %T", level, value)
		}
		value = array[0]
	}
	if value != leaf {
		t.Fatalf("nested leaf = %#v, want %q", value, leaf)
	}
}

// requireLoggedFieldLeaf decodes the structured fields that follow " fields:"
// in a formatted log line and checks the nested "deep" value.
func requireLoggedFieldLeaf(t *testing.T, line string, depth int, leaf string) {
	t.Helper()
	_, fieldsJSON, found := strings.Cut(line, " fields:")
	if !found {
		t.Fatalf("no structured fields in output: %.200s", line)
	}
	var fields map[string]any
	if err := json.Unmarshal([]byte(fieldsJSON), &fields); err != nil {
		t.Fatalf("decode logged fields: %v", err)
	}
	requireNestedRuntimeLeaf(t, fields["deep"], depth, leaf)
}

func TestPublicAnyPreservesAcceptedNestedValuesInOutput(t *testing.T) {
	for _, depth := range []int{2, heterolog.MaxFieldNestingDepth} {
		t.Run(fmt.Sprintf("depth_%d", depth), func(t *testing.T) {
			field, err := heterolog.Any("deep", nestedRuntimeValue(depth, "public leaf"))
			if err != nil {
				t.Fatal(err)
			}
			output := captureRuntimeStdout(t, func() {
				logger, err := heterolog.New(context.Background(), heterolog.DefaultConfig())
				if err != nil {
					t.Fatal(err)
				}
				defer logger.Close()
				if err := logger.Info(context.Background(), "nested value", field); err != nil {
					t.Fatal(err)
				}
				if err := logger.Close(); err != nil {
					t.Fatal(err)
				}
			})
			requireLoggedFieldLeaf(t, output, depth, "public leaf")
		})
	}
}

func TestPublicAnyRejectsValuesNestedBeyondMaximum(t *testing.T) {
	// Only Any's own result is asserted; the JSON encoder's limits are not.
	for _, depth := range []int{heterolog.MaxFieldNestingDepth + 1, 10001} {
		t.Run(fmt.Sprintf("depth_%d", depth), func(t *testing.T) {
			field, err := heterolog.Any("deep", nestedRuntimeValue(depth, "rejected leaf"))
			if !errors.Is(err, heterolog.ErrFieldValueNotJSON) {
				t.Fatalf("Any error = %v, want ErrFieldValueNotJSON", err)
			}
			if field.IsDefined() {
				t.Fatal("rejected value returned a usable field")
			}
		})
	}
}

// depthReplayTransport refuses the first Loki push and accepts later ones, so
// an accepted entry is stored in the spool and then replayed from it.
type depthReplayTransport struct {
	mu     sync.Mutex
	bodies [][]byte
}

func (tr *depthReplayTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	tr.mu.Lock()
	tr.bodies = append(tr.bodies, body)
	first := len(tr.bodies) == 1
	tr.mu.Unlock()
	status := http.StatusNoContent
	if first {
		status = http.StatusServiceUnavailable
	}
	return &http.Response{
		StatusCode: status, Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader("")), Request: req,
	}, nil
}

func TestPublicAnyMaximumNestingSurvivesSpoolRoundTrip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const leaf = "spooled leaf"
		depth := heterolog.MaxFieldNestingDepth
		field, err := heterolog.Any("deep", nestedRuntimeValue(depth, leaf))
		if err != nil {
			t.Fatal(err)
		}
		root := t.TempDir()
		transport := &depthReplayTransport{}
		cfg := runtimeMemoryPressureConfig(root, transport)
		cfg.Loki.NoBulkFlushModeEnabled = true
		cfg.Loki.FailureStoreConfig.DurableStoreConfig.SpoolConfig.OverloadPolicyConfig.UseMemoryPressureForOverload = false
		spool := heterolog.DefaultSpoolConfig()
		spool.RunnerConfig.LokiConfig.Runner.Interval = time.Second
		logger, err := heterolog.New(context.Background(), cfg, heterolog.WithSpool(spool))
		if err != nil {
			t.Fatal(err)
		}
		defer logger.Close()
		// The refused push stores the accepted entry in the spool.
		if err := logger.Info(context.Background(), "deep spool entry", field); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		// The replay worker decodes the stored record and sends it again.
		time.Sleep(2 * spool.RunnerConfig.LokiConfig.Runner.Interval)
		synctest.Wait()
		if err := logger.Close(); err != nil {
			t.Fatal(err)
		}

		transport.mu.Lock()
		bodies := slices.Clone(transport.bodies)
		transport.mu.Unlock()
		if len(bodies) != 2 {
			t.Fatalf("Loki pushes = %d, want the refused send and its replay", len(bodies))
		}
		var push struct {
			Streams []struct {
				Values [][]string `json:"values"`
			} `json:"streams"`
		}
		if err := json.Unmarshal(bodies[1], &push); err != nil {
			t.Fatalf("decode replayed push: %v", err)
		}
		if len(push.Streams) != 1 || len(push.Streams[0].Values) != 1 || len(push.Streams[0].Values[0]) != 2 {
			t.Fatalf("replayed push does not contain one entry: %.200s", bodies[1])
		}
		requireLoggedFieldLeaf(t, push.Streams[0].Values[0][1], depth, leaf)

		// The finalized record keeps the structured value at full depth.
		records := readRuntimeMemorySpoolRecords(t, filepath.Join(root, "memory-test", "spool", "loki", string(pkgspool.StateDone)))
		if len(records) != 1 {
			t.Fatalf("done spool records = %d, want the replayed entry", len(records))
		}
		var payload struct {
			Fields map[string]any `json:"fields"`
		}
		if err := json.Unmarshal(records[0].Payload, &payload); err != nil {
			t.Fatalf("decode stored payload: %v", err)
		}
		requireNestedRuntimeLeaf(t, payload.Fields["deep"], depth, leaf)
	})
}

func TestPublicSlogAttributesShareFieldNestingLimit(t *testing.T) {
	for _, depth := range []int{heterolog.MaxFieldNestingDepth, heterolog.MaxFieldNestingDepth + 1} {
		t.Run(fmt.Sprintf("depth_%d", depth), func(t *testing.T) {
			capture := &slogCaptureLogger{}
			handler := heterolog.NewSlogHandler(capture, nil)
			record := slog.NewRecord(time.Now(), slog.LevelInfo, "nested attribute", 0)
			record.AddAttrs(slog.Any("deep", nestedRuntimeValue(depth, "slog leaf")))
			err := handler.Handle(context.Background(), record)
			records := capture.snapshot()
			if depth > heterolog.MaxFieldNestingDepth {
				if !errors.Is(err, heterolog.ErrFieldValueNotJSON) || len(records) != 0 {
					t.Fatalf("Handle() = %v with %d records, want ErrFieldValueNotJSON before logging", err, len(records))
				}
				return
			}
			if err != nil || len(records) != 1 {
				t.Fatalf("Handle() = %v with %d records, want one logged record", err, len(records))
			}
			requireNestedRuntimeLeaf(t, records[0].fields["deep"], depth, "slog leaf")
		})
	}
}
