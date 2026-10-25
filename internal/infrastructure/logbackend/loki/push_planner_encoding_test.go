package loki

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

func TestPlanPushRequestsLimitsJSONDecodedLinesWithoutLosingNeighbor(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, tc := range []struct {
			name, input, received string
			reject                bool
		}{
			{"ASCII boundary", strings.Repeat("a", 16), strings.Repeat("a", 16), false},
			{"multibyte boundary", "日本語日本a", "日本語日本a", false},
			{"multibyte over", "日本語日本ab", "", true},
			{"invalid UTF8 boundary", strings.Repeat("\xff", 5) + "a", strings.Repeat("\ufffd", 5) + "a", false},
			{"invalid UTF8 over", strings.Repeat("\xff", 8), "", true},
			{"JSON escaping", strings.Repeat("\n\"<>", 4), strings.Repeat("\n\"<>", 4), false},
		} {
			t.Run(fmt.Sprintf("%s/gzip=%v", tc.name, compressed), func(t *testing.T) {
				records := []PushRecord{testPushRecord(0, tc.input), testPushRecord(1, "normal neighbor")}
				plan, err := PlanPushRequests(records, PushLimits{MaxLineBytes: 16, MaxPushRequestBytes: 1024})
				if err != nil {
					t.Fatal(err)
				}
				want := []string{tc.received, "normal neighbor"}
				if tc.reject {
					if len(plan.Rejected) != 1 || plan.Rejected[0].Record.InputIndex != 0 || !errors.Is(plan.Rejected[0].Err, ErrLogLineTooLarge) {
						t.Fatalf("rejected=%+v, want only oversized input 0", plan.Rejected)
					}
					want = want[1:]
				} else if len(plan.Rejected) != 0 {
					t.Fatalf("within-limit line rejected: %+v", plan.Rejected)
				}
				var received []string
				client, err := pkgloki.New(pkgloki.Config{Endpoint: "https://loki.example.test", CompressRequest: compressed,
					HTTPClient: &http.Client{Transport: encodingLimitTransport(func(r *http.Request) (*http.Response, error) {
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
							Streams []struct {
								Values [][2]string `json:"values"`
							} `json:"streams"`
						}
						if err := json.NewDecoder(body).Decode(&payload); err != nil {
							return nil, err
						}
						for _, stream := range payload.Streams {
							for _, value := range stream.Values {
								if len(value[1]) > 16 {
									t.Errorf("received oversized %d-byte line", len(value[1]))
								}
								if value[0] != strconv.FormatInt(records[0].Entry.Timestamp.UnixNano(), 10) {
									t.Errorf("timestamp changed: %q", value[0])
								}
								received = append(received, value[1])
							}
						}
						return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
					})},
				})
				if err != nil {
					t.Fatal(err)
				}
				for _, request := range plan.Requests {
					if err := client.Push(context.Background(), request.Streams); err != nil {
						t.Fatal(err)
					}
				}
				if len(received) != len(want) {
					t.Fatalf("received=%q, want=%q", received, want)
				}
				for i := range want {
					if received[i] != want[i] {
						t.Fatalf("received[%d]=%q, want %q", i, received[i], want[i])
					}
				}
			})
		}
	}
}

type encodingLimitTransport func(*http.Request) (*http.Response, error)

func (f encodingLimitTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
