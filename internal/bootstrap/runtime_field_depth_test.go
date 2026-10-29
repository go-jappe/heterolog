package bootstrap_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// nestedRuntimeValueは、leafをdepth重の配列で包みます。
func nestedRuntimeValue(depth int, leaf string) any {
	var value any = leaf
	for range depth {
		value = []any{value}
	}
	return value
}

// requireNestedRuntimeLeafは、要素が一つの配列をdepth重だけ外し、leafを確かめます。
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

// requireLoggedFieldLeafは、整形したlogの行で" fields:"に続く構造化fieldを
// decodeし、入れ子の"deep"の値を確かめます。
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

func TestAnyPreservesAcceptedNestedValuesInOutput(t *testing.T) {
	for _, depth := range []int{2, pkglogger.MaxFieldNestingDepth} {
		t.Run(fmt.Sprintf("depth_%d", depth), func(t *testing.T) {
			field, err := pkglogger.Any("deep", nestedRuntimeValue(depth, "public leaf"))
			if err != nil {
				t.Fatal(err)
			}
			output := testutil.CaptureStdout(t, func() {
				logger, err := bootstrap.New(context.Background(), loggerruntime.DefaultConfig())
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

func TestAnyRejectsValuesNestedBeyondMaximum(t *testing.T) {
	// 確かめるのはAny自体の結果だけで、JSONのencoderの上限は確かめません。
	for _, depth := range []int{pkglogger.MaxFieldNestingDepth + 1, 10001} {
		t.Run(fmt.Sprintf("depth_%d", depth), func(t *testing.T) {
			field, err := pkglogger.Any("deep", nestedRuntimeValue(depth, "rejected leaf"))
			if !errors.Is(err, pkglogger.ErrFieldValueNotJSON) {
				t.Fatalf("Any error = %v, want ErrFieldValueNotJSON", err)
			}
			if field.IsDefined() {
				t.Fatal("rejected value returned a usable field")
			}
		})
	}
}

// depthReplayTransportは、最初のLokiへのpushを拒否し、以後を受け付けます。
// 受け付けたlogはspoolに保存され、そこから再送されます。
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

func TestAnyMaximumNestingSurvivesSpoolRoundTrip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const leaf = "spooled leaf"
		depth := pkglogger.MaxFieldNestingDepth
		field, err := pkglogger.Any("deep", nestedRuntimeValue(depth, leaf))
		if err != nil {
			t.Fatal(err)
		}
		root := t.TempDir()
		transport := &depthReplayTransport{}
		cfg := runtimeMemoryPressureConfig(root, transport)
		cfg.Loki.NoBulkFlushModeEnabled = true
		cfg.Loki.FailureStoreConfig.DurableStoreConfig.SpoolConfig.OverloadPolicyConfig.UseMemoryPressureForOverload = false
		spool := loggerruntime.DefaultSpoolConfig()
		spool.RunnerConfig.LokiConfig.Runner.Interval = time.Second
		logger, err := bootstrap.New(context.Background(), cfg, loggerruntime.WithSpool(spool))
		if err != nil {
			t.Fatal(err)
		}
		defer logger.Close()
		// 拒否されたpushで、受け付けたlogがspoolに保存されます。
		if err := logger.Info(context.Background(), "deep spool entry", field); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		// 再送のworkerは、保存した記録をdecodeして送り直します。
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

		// 完了した記録は、構造化した値を入れ子の深さを保ったまま持ちます。
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
