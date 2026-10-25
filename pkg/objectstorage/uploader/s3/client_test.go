package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestClientUploadPutsFileToObjectKeyAndSignsRequest(t *testing.T) {
	t.Parallel()

	filePath := filepath.Join(t.TempDir(), "rotated.log.gz")
	if err := os.WriteFile(filePath, []byte("rotated log body"), 0o600); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	var gotPath string
	var gotBody string
	var gotAuthorization string
	var gotPayloadHash string
	var gotContentLength int64
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotPath = r.URL.EscapedPath()
		gotAuthorization = r.Header.Get("Authorization")
		gotPayloadHash = r.Header.Get("X-Amz-Content-Sha256")
		gotContentLength = r.ContentLength
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		gotBody = string(body)

		if r.Method != http.MethodPut {
			t.Fatalf("method = %s, want PUT", r.Method)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
			Request:    r,
		}, nil
	})

	client, err := New(Config{
		S3Config: objectstorage.S3Config{
			Region:          "ap-northeast-1",
			Endpoint:        "http://s3.local",
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
			Bucket:          "archive-bucket",
			ForcePathStyle:  true,
			Timeout:         time.Second,
		},
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	err = client.Upload(context.Background(), uploadertest.Input(t, filePath, "logs/default/rotated.log.gz"))
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}

	if gotPath != "/archive-bucket/logs/default/rotated.log.gz" {
		t.Fatalf("request path = %q, want %q", gotPath, "/archive-bucket/logs/default/rotated.log.gz")
	}
	if gotBody != "rotated log body" {
		t.Fatalf("request body = %q, want %q", gotBody, "rotated log body")
	}
	if gotAuthorization == "" {
		t.Fatalf("Authorization header is empty")
	}
	if gotPayloadHash != "UNSIGNED-PAYLOAD" {
		t.Fatalf("X-Amz-Content-Sha256 = %q, want UNSIGNED-PAYLOAD", gotPayloadHash)
	}
	if gotContentLength != int64(len("rotated log body")) {
		t.Fatalf("ContentLength = %d, want %d", gotContentLength, len("rotated log body"))
	}
}

type roundTripFunc func(r *http.Request) (*http.Response, error)

func TestClientUploadSendsExactContentLengthOnWire(t *testing.T) {
	for _, body := range []string{"", "compressed archive bytes"} {
		t.Run(strconv.Itoa(len(body))+" bytes", func(t *testing.T) {
			filePath := filepath.Join(t.TempDir(), "archive.gz")
			if err := os.WriteFile(filePath, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			type receivedRequest struct {
				method, host, uri, body string
				header                  http.Header
				length                  int64
				transferEncoding        []string
				readErr                 error
			}
			received := make(chan receivedRequest, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				payload, err := io.ReadAll(r.Body)
				received <- receivedRequest{
					method: r.Method, host: r.Host, uri: r.RequestURI, body: string(payload),
					header: r.Header.Clone(), length: r.ContentLength,
					transferEncoding: append([]string(nil), r.TransferEncoding...), readErr: err,
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			client, err := New(Config{
				S3Config: objectstorage.S3Config{
					Region: "ap-northeast-1", Endpoint: server.URL, Bucket: "archive-bucket", ForcePathStyle: true,
					AccessKeyID: "test-access", SecretAccessKey: "test-secret", SessionToken: "test-session",
				},
				HTTPClient: server.Client(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := client.Upload(context.Background(), uploadertest.Input(t, filePath, "archive.gz")); err != nil {
				t.Fatal(err)
			}
			got := <-received
			if got.readErr != nil {
				t.Fatal(got.readErr)
			}
			if got.method != http.MethodPut || got.uri != "/archive-bucket/archive.gz" || got.body != body {
				t.Fatalf("request method/URI/body = %q/%q/%q", got.method, got.uri, got.body)
			}
			if got.length != int64(len(body)) || got.header.Get("Content-Length") != strconv.Itoa(len(body)) || len(got.transferEncoding) != 0 {
				t.Errorf("wire ContentLength=%d Content-Length=%q TransferEncoding=%v, want explicit length %d without chunked encoding", got.length, got.header.Get("Content-Length"), got.transferEncoding, len(body))
			}
			assertS3RequestSignature(t, got.header, got.host, got.uri, len(body))
			// Upload borrows the source file; completion must leave its contents intact.
			source, err := os.ReadFile(filePath)
			if err != nil || string(source) != body {
				t.Fatalf("source after upload = %q, %v; want %q", source, err, body)
			}
		})
	}
}

// 301/302/303ではnet/httpがPUTを本文なしのGETへ変えて追従し、空本文なら307/308も
// 同じ署名で再送します。redirect先のGETが成功してもupload成功にせず、本文の有無に
// かかわらず元のPUT一回だけでredirectをHTTPStatusErrorとして返す必要があります。
func TestClientUploadReturnsRedirectWithoutFollowingOrChangingHTTPClientPolicy(t *testing.T) {
	statuses := []int{
		http.StatusMovedPermanently,
		http.StatusFound,
		http.StatusSeeOther,
		http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect,
	}
	for _, body := range []string{"", "compressed archive bytes"} {
		for _, status := range statuses {
			t.Run(fmt.Sprintf("%d/%d bytes", status, len(body)), func(t *testing.T) {
				filePath := filepath.Join(t.TempDir(), "archive.gz")
				if err := os.WriteFile(filePath, []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
				wantCode := ErrorCode("PermanentRedirect")
				if status == http.StatusTemporaryRedirect {
					wantCode = ErrorCodeTemporaryRedirect
				}
				var mu sync.Mutex
				var requests []string
				var policyCalls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/check-client-policy":
						w.Header().Set("Location", "/client-policy-preserved")
						w.WriteHeader(http.StatusTemporaryRedirect)
						return
					case "/client-policy-preserved":
						w.WriteHeader(http.StatusNoContent)
						return
					}
					payload, err := io.ReadAll(r.Body)
					if err != nil {
						t.Errorf("read request body: %v", err)
					}
					mu.Lock()
					requests = append(requests, r.Method+" "+r.URL.Path+" "+string(payload))
					mu.Unlock()
					if r.URL.Path != "/archive-bucket/archive.gz" {
						// proxyやSSO pageのように、redirect先は本文なしのGETにも成功を返します。
						w.WriteHeader(http.StatusOK)
						return
					}
					w.Header().Set("Location", "/redirected-upload")
					w.WriteHeader(status)
					_, _ = io.WriteString(w, "<Error><Code>"+string(wantCode)+"</Code></Error>")
				}))
				defer server.Close()
				httpClient := server.Client()
				httpClient.CheckRedirect = func(*http.Request, []*http.Request) error {
					policyCalls.Add(1)
					return nil
				}
				client, err := New(Config{
					S3Config: objectstorage.S3Config{
						Region: "ap-northeast-1", Endpoint: server.URL, Bucket: "archive-bucket", ForcePathStyle: true,
						AccessKeyID: "test-access", SecretAccessKey: "test-secret",
					},
					HTTPClient: httpClient,
				})
				if err != nil {
					t.Fatal(err)
				}
				err = client.Upload(context.Background(), uploadertest.Input(t, filePath, "archive.gz"))
				var statusErr *HTTPStatusError
				if !errors.As(err, &statusErr) || statusErr.Code != status || statusErr.ErrorCode != wantCode {
					t.Fatalf("Upload() = %v, want parsed HTTP status %d with code %s returned for classification", err, status, wantCode)
				}
				mu.Lock()
				gotRequests := slices.Clone(requests)
				mu.Unlock()
				if want := []string{"PUT /archive-bucket/archive.gz " + body}; !slices.Equal(gotRequests, want) || policyCalls.Load() != 0 {
					t.Fatalf("requests=%q policy calls=%d, want only %q without following the redirect", gotRequests, policyCalls.Load(), want)
				}
				// A shared client must retain the caller's redirect policy after Upload.
				resp, err := httpClient.Get(server.URL + "/check-client-policy")
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusNoContent || policyCalls.Load() != 1 {
					t.Errorf("shared client status=%d policy calls=%d, want 204 and original policy invoked once", resp.StatusCode, policyCalls.Load())
				}
			})
		}
	}
}

func TestClientUploadKeepsSourcePathAfterWorkingDirectoryChanges(t *testing.T) {
	originalDirectory := t.TempDir()
	otherDirectory := t.TempDir()
	const name = "App.log.gz"
	if err := os.WriteFile(filepath.Join(originalDirectory, name), []byte("original file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(otherDirectory, name), []byte("wrong file"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(originalDirectory)
	request, err := uploader.NewUploadRequest(name)
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(otherDirectory)
	input, err := uploader.NewUploadInput(request, "logs/"+name)
	if err != nil {
		t.Fatal(err)
	}
	bodies := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read uploaded body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		bodies <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client, err := New(Config{
		S3Config: objectstorage.S3Config{
			Region: "ap-northeast-1", Endpoint: server.URL, Bucket: "archive-bucket",
			AccessKeyID: "test-access-key", SecretAccessKey: "test-secret-key", ForcePathStyle: true,
		},
		HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Upload(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if got := <-bodies; got != "original file" {
		t.Fatalf("uploaded body = %q, want file selected when request was created", got)
	}
}

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestNewRejectsMissingBucketWhenEnabledConfigIsUsed(t *testing.T) {
	t.Parallel()

	_, err := New(Config{
		S3Config: objectstorage.S3Config{
			Region:          "ap-northeast-1",
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
		},
	})
	if err == nil {
		t.Fatalf("New() error = nil, want missing bucket error")
	}
}

func TestClientUploadReturnsHTTPStatusError(t *testing.T) {
	t.Parallel()

	filePath := filepath.Join(t.TempDir(), "rotated.log.gz")
	if err := os.WriteFile(filePath, []byte("rotated log body"), 0o600); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body := `<Error>` +
			`<Code>RequestTimeout</Code>` +
			`<Message>Your socket connection to the server was not read from or written to within the timeout period.</Message>` +
			`<Resource>/archive-bucket/rotated.log.gz</Resource>` +
			`<RequestId>request-from-xml</RequestId>` +
			`<HostId>host-from-xml</HostId>` +
			`</Error>`
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Status:     "503 Service Unavailable",
			Body:       io.NopCloser(strings.NewReader(body)),
			Header: http.Header{
				"Retry-After":      []string{"2"},
				"X-Amz-Request-Id": []string{"request-from-header"},
				"X-Amz-Id-2":       []string{"host-from-header"},
			},
			Request: r,
		}, nil
	})

	client, err := New(Config{
		S3Config: objectstorage.S3Config{
			Region:          "ap-northeast-1",
			Endpoint:        "http://s3.local",
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
			Bucket:          "archive-bucket",
		},
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	err = client.Upload(context.Background(), uploadertest.Input(t, filePath, "rotated.log.gz"))
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("Upload() error = %T, want HTTPStatusError", err)
	}
	if statusErr.StatusCode() != http.StatusServiceUnavailable {
		t.Fatalf("StatusCode() = %d, want %d", statusErr.StatusCode(), http.StatusServiceUnavailable)
	}
	if statusErr.RetryAfter() != 2*time.Second {
		t.Fatalf("RetryAfter() = %s, want 2s", statusErr.RetryAfter())
	}
	if statusErr.ErrorCode != "RequestTimeout" {
		t.Fatalf("ErrorCode = %q, want RequestTimeout", statusErr.ErrorCode)
	}
	if statusErr.Message == "" {
		t.Fatal("Message is empty")
	}
	if statusErr.Resource != "/archive-bucket/rotated.log.gz" {
		t.Fatalf("Resource = %q", statusErr.Resource)
	}
	if statusErr.RequestID != "request-from-xml" {
		t.Fatalf("RequestID = %q, want XML value", statusErr.RequestID)
	}
	if statusErr.HostID != "host-from-xml" {
		t.Fatalf("HostID = %q, want XML value", statusErr.HostID)
	}
	if statusErr.ResponseParseError != nil {
		t.Fatalf("ResponseParseError = %v", statusErr.ResponseParseError)
	}
}

func TestClientUploadDoesNotRetryRetryableS3Response(t *testing.T) {
	t.Parallel()

	filePath := filepath.Join(t.TempDir(), "rotated.log.gz")
	if err := os.WriteFile(filePath, []byte("rotated log body"), 0o600); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	requestCount := 0
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requestCount++
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Status:     "503 Service Unavailable",
			Body: io.NopCloser(strings.NewReader(
				`<Error><Code>ServiceUnavailable</Code></Error>`,
			)),
			Header:  make(http.Header),
			Request: r,
		}, nil
	})

	client, err := New(Config{
		S3Config: objectstorage.S3Config{
			Region:          "ap-northeast-1",
			Endpoint:        "http://s3.local",
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
			Bucket:          "archive-bucket",
		},
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	err = client.Upload(context.Background(), uploadertest.Input(t, filePath, "rotated.log.gz"))
	if err == nil {
		t.Fatal("Upload() error = nil, want retryable S3 response error")
	}
	if requestCount != 1 {
		t.Fatalf("S3 PUT request count = %d, want exactly 1", requestCount)
	}
}

func TestClientUploadReturnsTemporaryRedirectForRetryClassification(t *testing.T) {
	t.Parallel()

	filePath := filepath.Join(t.TempDir(), "rotated.log.gz")
	if err := os.WriteFile(filePath, []byte("rotated log body"), 0o600); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	requestCount := 0
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requestCount++
		return &http.Response{
			StatusCode: http.StatusTemporaryRedirect,
			Status:     "307 Temporary Redirect",
			Body: io.NopCloser(strings.NewReader(
				`<Error><Code>TemporaryRedirect</Code>` +
					`<Message>Please re-send this request to the specified temporary endpoint.</Message>` +
					`<Endpoint>temporary.s3.local</Endpoint></Error>`,
			)),
			Header: http.Header{
				"Location": []string{"http://temporary.s3.local/archive-bucket/rotated.log.gz"},
			},
			Request: r,
		}, nil
	})

	client, err := New(Config{
		S3Config: objectstorage.S3Config{
			Region:          "ap-northeast-1",
			Endpoint:        "http://s3.local",
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
			Bucket:          "archive-bucket",
		},
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	err = client.Upload(context.Background(), uploadertest.Input(t, filePath, "rotated.log.gz"))
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("Upload() error = %T, want HTTPStatusError", err)
	}
	if statusErr.StatusCode() != http.StatusTemporaryRedirect ||
		statusErr.ErrorCode != ErrorCodeTemporaryRedirect {
		t.Fatalf("status error = status %d code %q, want 307 TemporaryRedirect", statusErr.StatusCode(), statusErr.ErrorCode)
	}
	// UploadはPUTのredirectを追従しないため、307もdurable再送用classifierが
	// 判定できるHTTPStatusErrorとして返します。
	if requestCount != 1 {
		t.Fatalf("request count = %d, want 1 without automatic redirect", requestCount)
	}
}

func TestNewHTTPStatusErrorKeepsMalformedXMLAsBoundedDiagnosticBody(t *testing.T) {
	t.Parallel()

	body := []byte(`<Error><Code>RequestTimeout</Code>`)
	statusErr := newHTTPStatusError(
		&http.Response{
			StatusCode: http.StatusBadRequest,
			Status:     "400 Bad Request",
			Header: http.Header{
				"X-Amz-Request-Id": []string{"request-from-header"},
				"X-Amz-Id-2":       []string{"host-from-header"},
			},
		},
		body,
		false,
		nil,
	)

	if statusErr.ResponseParseError == nil {
		t.Fatal("ResponseParseError = nil, want malformed XML error")
	}
	if statusErr.ErrorCode != "" || statusErr.Message != "" {
		t.Fatalf("parsed fields = code %q message %q, want empty", statusErr.ErrorCode, statusErr.Message)
	}
	if !bytes.Equal(statusErr.Body, body) {
		t.Fatalf("Body = %q, want %q", statusErr.Body, body)
	}
	if statusErr.RequestID != "request-from-header" || statusErr.HostID != "host-from-header" {
		t.Fatalf("header identity = request %q host %q", statusErr.RequestID, statusErr.HostID)
	}
}

func TestReadHTTPStatusErrorBodyLimitsOversizedXML(t *testing.T) {
	t.Parallel()

	oversized := strings.Repeat("x", int(maxHTTPStatusErrorBodyBytes)+1024)
	body, truncated, err := readHTTPStatusErrorBody(strings.NewReader(oversized))
	if err != nil {
		t.Fatalf("readHTTPStatusErrorBody() error = %v", err)
	}
	if !truncated {
		t.Fatal("truncated = false, want true")
	}
	if got := int64(len(body)); got != maxHTTPStatusErrorBodyBytes {
		t.Fatalf("body length = %d, want %d", got, maxHTTPStatusErrorBodyBytes)
	}

	statusErr := newHTTPStatusError(
		&http.Response{StatusCode: http.StatusBadRequest, Header: make(http.Header)},
		body,
		truncated,
		nil,
	)
	if !statusErr.BodyTruncated {
		t.Fatal("BodyTruncated = false, want true")
	}
	if statusErr.ResponseParseError == nil {
		t.Fatal("ResponseParseError = nil, want non-XML body error")
	}
}

// time.Durationで表せないRetry-Afterは、HTTP-dateと同じく最大値へ飽和させ、呼び出し側の
// 待機上限を適用させます。0にすると、長い待機を求めたserverへ即時再送してしまいます。
func TestParseRetryAfterSaturatesDurationOverflow(t *testing.T) {
	t.Parallel()

	const maxDelay = time.Duration(1<<63 - 1)
	for _, tt := range []struct {
		value string
		want  time.Duration
	}{
		{value: "9223372036", want: 9223372036 * time.Second},
		{value: "9223372037", want: maxDelay},
		{value: "99999999999999999999", want: maxDelay},
		{value: "Fri, 31 Dec 9999 23:59:59 GMT", want: maxDelay},
		{value: "12a", want: 0},
		{value: "-1", want: 0},
	} {
		if got := parseRetryAfter(tt.value); got != tt.want {
			t.Errorf("parseRetryAfter(%q) = %s, want %s", tt.value, got, tt.want)
		}
	}
}
