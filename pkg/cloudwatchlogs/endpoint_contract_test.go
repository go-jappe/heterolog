package cloudwatchlogs_test

import (
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	cw "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

type endpointRoundTripFunc func(*http.Request) (*http.Response, error)

func (f endpointRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestRegionEndpointAndSignatureMatchOutgoingRequests(t *testing.T) {
	for _, tt := range []struct {
		name, region, endpoint, host, path string
	}{
		{"Tokyo", "ap-northeast-1", "", "logs.ap-northeast-1.amazonaws.com", "/"},
		{"GovCloud", "us-gov-west-1", "", "logs.us-gov-west-1.amazonaws.com", "/"},
		{"Beijing", "cn-north-1", "", "logs.cn-north-1.amazonaws.com.cn", "/"},
		{"Ningxia", "cn-northwest-1", "", "logs.cn-northwest-1.amazonaws.com.cn", "/"},
		{"China override", "cn-north-1", "https://proxy.example.test/gateway/", "proxy.example.test", "/gateway"},
		{"Tokyo override", "ap-northeast-1", "https://proxy.example.test/gateway/", "proxy.example.test", "/gateway"},
	} {
		for _, compressed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/gzip=%v", tt.name, compressed), func(t *testing.T) {
				var targets []string
				transport := endpointRoundTripFunc(func(request *http.Request) (*http.Response, error) {
					body, err := io.ReadAll(request.Body)
					if err != nil {
						return nil, err
					}
					path := request.URL.EscapedPath()
					if path == "" {
						path = "/"
					}
					if request.Method != http.MethodPost || request.URL.Scheme != "https" ||
						request.URL.Host != tt.host || request.Host != tt.host || path != tt.path || request.URL.RawQuery != "" {
						t.Errorf("request = %s %s Host:%s, want POST https://%s%s", request.Method, request.URL, request.Host, tt.host, tt.path)
					}
					assertEndpointRequestSignature(t, request.Header, body, tt.region, tt.host, tt.path)
					var decoded io.Reader = strings.NewReader(string(body))
					if compressed {
						if request.Header.Get("Content-Encoding") != "gzip" {
							t.Error("missing gzip Content-Encoding")
						}
						reader, err := gzip.NewReader(decoded)
						if err != nil {
							return nil, err
						}
						defer reader.Close()
						decoded = reader
					}
					var payload struct {
						LogGroupName, LogStreamName string
						LogEvents                   []struct {
							Timestamp int64
							Message   string
						}
					}
					if err := json.NewDecoder(decoded).Decode(&payload); err != nil {
						return nil, err
					}
					target := request.Header.Get("X-Amz-Target")
					targets = append(targets, target)
					if payload.LogGroupName != "/app/test" || payload.LogStreamName != "stream" {
						t.Errorf("request destination changed: %+v", payload)
					}
					if strings.HasSuffix(target, ".PutLogEvents") &&
						(len(payload.LogEvents) != 1 || payload.LogEvents[0].Message != "event" || payload.LogEvents[0].Timestamp != 1_761_024_600_000) {
						t.Errorf("request events changed: %+v", payload.LogEvents)
					}
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
				})
				client, err := cw.New(cw.Config{
					Region: tt.region, Endpoint: tt.endpoint, LogGroupName: "/app/test",
					AccessKeyID: "test-access", SecretAccessKey: "test-secret", SessionToken: "test-session",
					CompressRequest: compressed, HTTPClient: &http.Client{Transport: transport},
				})
				if err != nil {
					t.Fatal(err)
				}
				when := time.UnixMilli(1_761_024_600_000)
				if _, err := client.PutLogEvents(context.Background(), cw.PutLogEventsRequest{
					LogStreamName: "stream", Events: []cw.Event{{Timestamp: when, Message: "event"}}, AttemptedAt: when,
				}); err != nil {
					t.Fatal(err)
				}
				if err := client.CreateLogStream(context.Background(), "stream"); err != nil {
					t.Fatal(err)
				}
				want := []string{"Logs_20140328.PutLogEvents", "Logs_20140328.CreateLogStream"}
				if !slices.Equal(targets, want) {
					t.Errorf("targets = %v, want %v", targets, want)
				}
			})
		}
	}
}

func TestRequestsPreserveEndpointPathSegments(t *testing.T) {
	for _, tt := range []struct {
		name, endpointPath, wantURI string
	}{
		{"base URL", "", "/"},
		{"base URL slashes", "///", "/"},
		{"proxy path", "/gateway", "/gateway"},
		{"proxy trailing delimiters", "/gateway///", "/gateway"},
		{"encoded slash", "/tenant%2Fprod/gateway", "/tenant%2Fprod/gateway"},
		{"encoded slash and delimiter", "/tenant%2Fprod/gateway/", "/tenant%2Fprod/gateway"},
		{"encoded lowercase slash", "/tenant%2fprod/gateway//", "/tenant%2fprod/gateway"},
		{"space and percent", "/tenant%20one%25/gateway/", "/tenant%20one%25/gateway"},
		{"encoded terminal slash", "/gateway%2F", "/gateway%2F"},
		{"encoded terminal slash and delimiter", "/gateway%2F/", "/gateway%2F"},
		{"encoded root slash", "/%2F/", "/%2F"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var targets []string
			transport := endpointRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				if request.Method != http.MethodPost || request.URL.Host != "proxy.example.test" || request.URL.RequestURI() != tt.wantURI {
					t.Errorf("request = %s %s, want POST https://proxy.example.test%s", request.Method, request.URL, tt.wantURI)
				}
				var payload struct {
					LogGroupName, LogStreamName string
					LogEvents                   []struct {
						Timestamp int64
						Message   string
					}
				}
				if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
					t.Fatal(err)
				}
				target := request.Header.Get("X-Amz-Target")
				targets = append(targets, target)
				if payload.LogGroupName != "/app/test" || payload.LogStreamName != "stream" {
					t.Fatalf("request destination changed: %+v", payload)
				}
				if strings.HasSuffix(target, ".PutLogEvents") &&
					(len(payload.LogEvents) != 1 || payload.LogEvents[0].Message != "route-me" || payload.LogEvents[0].Timestamp != 1_761_024_600_000) {
					t.Fatalf("request events changed: %+v", payload)
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
			})
			client, err := cw.New(cw.Config{
				Region: "ap-northeast-1", Endpoint: "https://proxy.example.test" + tt.endpointPath,
				LogGroupName: "/app/test", AccessKeyID: "test-access", SecretAccessKey: "test-secret",
				HTTPClient: &http.Client{Transport: transport},
			})
			if err != nil {
				t.Fatal(err)
			}
			when := time.UnixMilli(1_761_024_600_000)
			if _, err := client.PutLogEvents(context.Background(), cw.PutLogEventsRequest{
				LogStreamName: "stream", Events: []cw.Event{{Timestamp: when, Message: "route-me"}}, AttemptedAt: when,
			}); err != nil {
				t.Fatal(err)
			}
			if err := client.CreateLogStream(context.Background(), "stream"); err != nil {
				t.Fatal(err)
			}
			if want := []string{"Logs_20140328.PutLogEvents", "Logs_20140328.CreateLogStream"}; !slices.Equal(targets, want) {
				t.Fatalf("targets = %v, want %v", targets, want)
			}
		})
	}
}

// Recompute SigV4 with the expected endpoint and region, without using the SDK
// signer or the production endpoint resolver. The payload hash covers wire bytes.
func assertEndpointRequestSignature(t *testing.T, headers http.Header, body []byte, region, host, path string) {
	t.Helper()
	date := headers.Get("X-Amz-Date")
	when, err := time.Parse("20060102T150405Z", date)
	if err != nil {
		t.Fatal(err)
	}
	if headers.Get("Content-Type") != "application/x-amz-json-1.1" || headers.Get("X-Amz-Security-Token") != "test-session" {
		t.Fatal("missing content type or session token")
	}
	authorization, ok := strings.CutPrefix(headers.Get("Authorization"), "AWS4-HMAC-SHA256 ")
	if !ok {
		t.Fatal("missing SigV4 Authorization")
	}
	parameters := make(map[string]string)
	for _, part := range strings.Split(authorization, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || parameters[key] != "" {
			t.Fatalf("invalid Authorization parameter %q", part)
		}
		parameters[key] = value
	}
	names := strings.Split(parameters["SignedHeaders"], ";")
	if !slices.IsSorted(names) {
		t.Fatal("SignedHeaders must be sorted")
	}
	for _, required := range []string{"host", "content-type", "x-amz-date", "x-amz-target", "x-amz-security-token"} {
		if !slices.Contains(names, required) {
			t.Fatalf("missing signed header %s", required)
		}
	}
	var canonicalHeaders strings.Builder
	for _, name := range names {
		values := slices.Clone(headers.Values(name))
		switch name {
		case "host":
			values = []string{host}
		case "content-length":
			values = []string{strconv.Itoa(len(body))}
		}
		for i, value := range values {
			values[i] = strings.Join(strings.Fields(value), " ")
		}
		fmt.Fprintf(&canonicalHeaders, "%s:%s\n", name, strings.Join(values, ","))
	}
	payloadHash := sha256.Sum256(body)
	canonicalRequest := strings.Join([]string{http.MethodPost, path, "", canonicalHeaders.String(), parameters["SignedHeaders"], fmt.Sprintf("%x", payloadHash)}, "\n")
	requestHash := sha256.Sum256([]byte(canonicalRequest))
	day := when.Format("20060102")
	scope := day + "/" + region + "/logs/aws4_request"
	if parameters["Credential"] != "test-access/"+scope {
		t.Fatalf("Credential = %q, want test-access/%s", parameters["Credential"], scope)
	}
	toSign := "AWS4-HMAC-SHA256\n" + date + "\n" + scope + "\n" + fmt.Sprintf("%x", requestHash)
	hash := func(key []byte, value string) []byte {
		mac := hmac.New(sha256.New, key)
		_, _ = mac.Write([]byte(value))
		return mac.Sum(nil)
	}
	key := []byte("AWS4test-secret")
	for _, value := range []string{day, region, "logs", "aws4_request"} {
		key = hash(key, value)
	}
	if want := fmt.Sprintf("%x", hash(key, toSign)); parameters["Signature"] != want {
		t.Errorf("Signature = %s, want independently computed %s", parameters["Signature"], want)
	}
}
