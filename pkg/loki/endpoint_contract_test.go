package loki_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/pkg/loki"
)

type endpointRoundTripFunc func(*http.Request) (*http.Response, error)

func (f endpointRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestPushPreservesEndpointPathSegments(t *testing.T) {
	for _, tt := range []struct {
		name, endpointPath, wantURI string
	}{
		{"base URL", "", "/loki/api/v1/push"},
		{"base URL slashes", "///", "/loki/api/v1/push"},
		{"full path", "/loki/api/v1/push", "/loki/api/v1/push"},
		{"proxy trailing delimiters", "/proxy/loki/api/v1/push///", "/proxy/loki/api/v1/push"},
		{"encoded slash", "/tenant%2Fprod/loki/api/v1/push", "/tenant%2Fprod/loki/api/v1/push"},
		{"encoded slash and delimiter", "/tenant%2Fprod/loki/api/v1/push/", "/tenant%2Fprod/loki/api/v1/push"},
		{"encoded lowercase slash", "/tenant%2fprod/loki/api/v1/push//", "/tenant%2fprod/loki/api/v1/push"},
		{"space and percent", "/tenant%20one%25/loki/api/v1/push/", "/tenant%20one%25/loki/api/v1/push"},
		{"encoded slash at end of prefix", "/tenant%2F/loki/api/v1/push/", "/tenant%2F/loki/api/v1/push"},
		{"encoded unreserved API characters", "/proxy/l%6Fki/api/v%31/p%75sh/", "/proxy/l%6Fki/api/v%31/p%75sh"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			transport := endpointRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				calls++
				if request.Method != http.MethodPost || request.URL.Host != "proxy.example.test" || request.URL.RequestURI() != tt.wantURI {
					t.Errorf("request = %s %s, want POST https://proxy.example.test%s", request.Method, request.URL, tt.wantURI)
				}
				var payload struct {
					Streams []struct {
						Stream map[string]string `json:"stream"`
						Values [][2]string       `json:"values"`
					} `json:"streams"`
				}
				if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
					t.Fatal(err)
				}
				if len(payload.Streams) != 1 || payload.Streams[0].Stream["app"] != "path-test" ||
					len(payload.Streams[0].Values) != 1 || payload.Streams[0].Values[0] != [2]string{"1700000000000000123", "route-me"} {
					t.Fatalf("request payload changed: %+v", payload)
				}
				return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader(""))}, nil
			})
			client, err := loki.New(loki.Config{
				Endpoint:   "https://proxy.example.test" + tt.endpointPath,
				HTTPClient: &http.Client{Transport: transport},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := client.PushEntry(context.Background(), map[string]string{"app": "path-test"}, loki.Entry{
				Timestamp: time.Unix(1_700_000_000, 123), Line: "route-me",
			}); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("requests = %d, want 1", calls)
			}
		})
	}
}

func TestNewRejectsEncodedPushPathSeparators(t *testing.T) {
	for _, path := range []string{
		"/%2F",                // Encoded root slash is a segment, not an empty path.
		"/loki%2Fapi/v1/push", // The API requires four distinct segments.
		"/loki/api%2Fv1/push/",
		"/loki/api/v1%2Fpush",
		"/proxy%2Floki/api/v1/push/", // The leading API delimiter must also be literal.
		"/loki/api/v1/push%2F",       // An encoded terminal slash belongs to "push/".
		"/loki/api/v1/push%2F/",
	} {
		t.Run(path, func(t *testing.T) {
			if _, err := loki.New(loki.Config{Endpoint: "https://proxy.example.test" + path}); err == nil {
				t.Fatal("expected invalid Push API path to be rejected")
			}
		})
	}
}
