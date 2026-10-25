package cloudwatchlogs

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestCloudWatchLogsFlushFitsDecodedUTF8WithoutRejectingNormalNeighbor(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("gzip=%v", compressed), func(t *testing.T) {
			entries := []*logging.LogEntry{
				newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, strings.Repeat("\xff", 120000)),
				newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, strings.Repeat("a", 700000)),
			}
			want := make([]string, len(entries))
			for i, entry := range entries {
				// Both messages fit the public entry limit; their combined decoded
				// messages exceed AWS's batch limit only after JSON replacement.
				size, err := applogging.MeasureLogEntrySize(entry.Message, nil)
				if err != nil || size > pkglogger.DefaultMaxLogEntryBytes {
					t.Fatalf("fixture entry %d does not fit the public limit: bytes=%d error=%v", i, size, err)
				}
				encoded, err := json.Marshal(cloudWatchLogsTestPlainTextFormatter.Format(entry))
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(encoded, &want[i]); err != nil {
					t.Fatal(err)
				}
			}
			if len(want[0])+len(want[1])+52 <= 1048576 {
				t.Fatal("fixture must require two physical requests")
			}
			calls, received := 0, 0
			client, err := pkgcloudwatchlogs.New(pkgcloudwatchlogs.Config{
				Region: "ap-northeast-1", Endpoint: "https://logs.example.test", LogGroupName: "logs",
				AccessKeyID: "test", SecretAccessKey: "test", CompressRequest: compressed,
				HTTPClient: &http.Client{Transport: encodingLimitTransport(func(r *http.Request) (*http.Response, error) {
					calls++
					var body io.Reader = r.Body
					if compressed {
						gz, err := gzip.NewReader(r.Body)
						if err != nil {
							return nil, err
						}
						defer gz.Close()
						body = gz
					}
					var payload struct {
						LogEvents []struct {
							Message   string `json:"message"`
							Timestamp int64  `json:"timestamp"`
						} `json:"logEvents"`
					}
					if err := json.NewDecoder(body).Decode(&payload); err != nil {
						return nil, err
					}
					total := 0
					for _, event := range payload.LogEvents {
						total += len(event.Message) + 26
					}
					status, response := http.StatusOK, "{}"
					if total > 1048576 {
						t.Errorf("sent oversized batch: %d decoded bytes", total)
						status, response = http.StatusBadRequest, `{"__type":"InvalidParameterException","message":"batch size exceeded"}`
					} else {
						for _, event := range payload.LogEvents {
							if received >= len(want) || event.Message != want[received] || event.Timestamp != entries[received].OccurredAt.UnixMilli() {
								t.Fatalf("event %d lost content or order (bytes=%d timestamp=%d)", received, len(event.Message), event.Timestamp)
							}
							received++
						}
					}
					return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response))}, nil
				})},
			})
			if err != nil {
				t.Fatal(err)
			}
			failure := &recordingCloudWatchLogsFailureHandler{}
			manager := newTestCloudWatchLogsFlushManager(client, mustNewCloudWatchLogsFailureComposite(t, []flushfailure.FailureHandler{failure}))
			if err := manager.Flush(context.Background(), entries); err != nil {
				t.Fatal(err)
			}
			if result, called := failure.result(); called {
				t.Fatalf("valid entries reached failure handling: retryable=%d dead=%d quarantine=%d", len(result.RetryableRecords), len(result.DeadRecords), len(result.QuarantineRecords))
			}
			if calls != 2 || received != len(want) {
				t.Fatalf("HTTP calls=%d accepted events=%d, want 2 requests and both events", calls, received)
			}
		})
	}
}

func TestCloudWatchLogsBufferedBytesMatchJSONDecodedMessage(t *testing.T) {
	entry := newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "日本\xff\xfe\ufffd\n\"<")
	encoded, err := json.Marshal(cloudWatchLogsTestPlainTextFormatter.Format(entry))
	if err != nil {
		t.Fatal(err)
	}
	var received string
	if err := json.Unmarshal(encoded, &received); err != nil {
		t.Fatal(err)
	}
	if got := cloudWatchLogsBufferedEntryBytes(entry, cloudWatchLogsTestPlainTextFormatter); got != len(received)+26 {
		t.Fatalf("buffered bytes=%d, want decoded message bytes + overhead=%d", got, len(received)+26)
	}
}

type encodingLimitTransport func(*http.Request) (*http.Response, error)

func (f encodingLimitTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
