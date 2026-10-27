package s3

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestUploadPreservesObjectKeyAndSignsTransmittedURI(t *testing.T) {
	const fileBody = "compressed archive bytes"
	filePath := filepath.Join(t.TempDir(), "archive.log.gz")
	if err := os.WriteFile(filePath, []byte(fileBody), 0o600); err != nil {
		t.Fatal(err)
	}
	keys := []struct {
		name    string
		key     string
		escaped string
	}{
		{name: "plain", key: "logs/plain.gz", escaped: "logs/plain.gz"},
		{name: "boundary spaces retain distinct key", key: " logs/plain.gz ", escaped: "%20logs/plain.gz%20"},
		{name: "boundary control whitespace", key: "\tlogs/plain.gz\n", escaped: "%09logs/plain.gz%0A"},
		{name: "only whitespace", key: " \t\n", escaped: "%20%09%0A"},
		{name: "unicode boundary whitespace", key: "\u3000logs/plain.gz\u3000", escaped: "%E3%80%80logs/plain.gz%E3%80%80"},
		{name: "space", key: "logs/my file.gz", escaped: "logs/my%20file.gz"},
		{name: "unicode", key: "logs/日本.gz", escaped: "logs/%E6%97%A5%E6%9C%AC.gz"},
		{name: "query and fragment", key: "logs/a?b#c.gz", escaped: "logs/a%3Fb%23c.gz"},
		{name: "literal escape", key: "logs/a%2Fb.gz", escaped: "logs/a%252Fb.gz"},
		{name: "reserved segment bytes", key: "logs/@:$&=+.gz", escaped: "logs/%40%3A%24%26%3D%2B.gz"},
		{name: "repeated slash", key: "logs//file.gz", escaped: "logs//file.gz"},
		{name: "dot segment", key: "logs/./file.gz", escaped: "logs/./file.gz"},
		{name: "parent segment", key: "logs/child/../file.gz", escaped: "logs/child/../file.gz"},
		{name: "leading slash", key: "/logs/file.gz", escaped: "/logs/file.gz"},
		{name: "trailing slash", key: "logs/", escaped: "logs/"},
	}
	prefixes := []struct {
		name    string
		encoded string
		decoded string
	}{
		{name: "root"},
		{name: "proxy prefix", encoded: "/gateway", decoded: "/gateway"},
		{name: "escaped proxy prefix", encoded: "/edge%2Fspace%20%2B", decoded: "/edge/space +"},
	}
	for _, pathStyle := range []bool{true, false} {
		style := "virtual host"
		if pathStyle {
			style = "path"
		}
		for _, prefix := range prefixes {
			t.Run(style+"/"+prefix.name, func(t *testing.T) {
				type receivedRequest struct {
					method, path, uri, host, body string
					header                        http.Header
					readErr                       error
				}
				requests := make(chan receivedRequest, 1)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
					body, err := io.ReadAll(request.Body)
					requests <- receivedRequest{
						method: request.Method, path: request.URL.Path, uri: request.RequestURI,
						host: request.Host, body: string(body), header: request.Header.Clone(), readErr: err,
					}
					w.WriteHeader(http.StatusOK)
				}))
				defer server.Close()
				// virtual-host形式のrequestを、requestとその署名には実際のbucketのhost名を
				// 残したまま、local serverへ送ります。
				transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
				}}
				defer transport.CloseIdleConnections()
				client, err := New(Config{
					S3Config: objectstorage.S3Config{
						Region: "ap-northeast-1", Endpoint: server.URL + prefix.encoded + "/",
						Bucket: "archive-bucket", ForcePathStyle: pathStyle,
						AccessKeyID: "test-access", SecretAccessKey: "test-secret", SessionToken: "test-session",
					},
					HTTPClient: &http.Client{Transport: transport, Timeout: 5 * time.Second},
				})
				if err != nil {
					t.Fatal(err)
				}
				wantHost := strings.TrimPrefix(server.URL, "http://")
				wantPathPrefix, wantURIPrefix := prefix.decoded, prefix.encoded
				if pathStyle {
					wantPathPrefix += "/archive-bucket"
					wantURIPrefix += "/archive-bucket"
				} else {
					wantHost = "archive-bucket." + wantHost
				}
				for _, key := range keys {
					t.Run(key.name, func(t *testing.T) {
						if err := client.Upload(context.Background(), uploadertest.Input(t, filePath, key.key)); err != nil {
							t.Fatal(err)
						}
						got := <-requests
						if got.readErr != nil {
							t.Fatal(got.readErr)
						}
						if got.method != http.MethodPut || got.body != fileBody || got.host != wantHost {
							t.Errorf("request method/body/host = %q/%q/%q, want PUT/%q/%q", got.method, got.body, got.host, fileBody, wantHost)
						}
						wantURI := wantURIPrefix + "/" + key.escaped
						if got.uri != wantURI || got.path != wantPathPrefix+"/"+key.key {
							t.Errorf("request URI/path = %q/%q, want %q/%q", got.uri, got.path, wantURI, wantPathPrefix+"/"+key.key)
						}
						assertS3RequestSignature(t, got.header, wantHost, wantURI, len(fileBody))
					})
				}
			})
		}
	}
}

// regionは前後の空白だけを除いた入力値でcredential scopeへ入れます。S3互換serviceの
// region名を変えないよう、大文字小文字は正規化しません。宛先IDだけがregionの大文字小文字を
// 同一視します。
func TestUploadSignsWithTrimmedInputRegion(t *testing.T) {
	const fileBody = "compressed archive bytes"
	filePath := filepath.Join(t.TempDir(), "archive.log.gz")
	if err := os.WriteFile(filePath, []byte(fileBody), 0o600); err != nil {
		t.Fatal(err)
	}
	type receivedRequest struct {
		host, uri string
		header    http.Header
		readErr   error
	}
	requests := make(chan receivedRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, err := io.Copy(io.Discard, request.Body)
		requests <- receivedRequest{host: request.Host, uri: request.RequestURI, header: request.Header.Clone(), readErr: err}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client, err := New(Config{
		S3Config: objectstorage.S3Config{
			Region: " Custom-Region-1 ", Endpoint: server.URL, Bucket: "archive-bucket", ForcePathStyle: true,
			AccessKeyID: "test-access", SecretAccessKey: "test-secret", SessionToken: "test-session",
		},
		HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Upload(context.Background(), uploadertest.Input(t, filePath, "logs/archive.log.gz")); err != nil {
		t.Fatal(err)
	}
	got := <-requests
	if got.readErr != nil {
		t.Fatal(got.readErr)
	}
	assertS3RequestSignatureForRegion(t, got.header, got.host, got.uri, len(fileBody), "Custom-Region-1")
}

// SDKのsignerや本番のURL helperとは独立に、期待する通信上の契約からSigV4を計算し直します。
// S3はencodeしたURIをちょうど一度だけ使います。
func assertS3RequestSignature(t *testing.T, headers http.Header, host, uri string, bodyLength int) {
	t.Helper()
	assertS3RequestSignatureForRegion(t, headers, host, uri, bodyLength, "ap-northeast-1")
}

// assertS3RequestSignatureForRegionは、regionをcredential scopeと署名鍵へ指定値のまま
// 使ったSigV4を再計算して比較します。
func assertS3RequestSignatureForRegion(t *testing.T, headers http.Header, host, uri string, bodyLength int, region string) {
	t.Helper()
	date := headers.Get("X-Amz-Date")
	when, err := time.Parse("20060102T150405Z", date)
	if err != nil {
		t.Fatalf("invalid signing date %q: %v", date, err)
	}
	if headers.Get("X-Amz-Content-Sha256") != "UNSIGNED-PAYLOAD" || headers.Get("X-Amz-Security-Token") != "test-session" {
		t.Fatal("missing or incorrect S3 payload hash/session token")
	}
	if headers.Get("Content-Type") != "application/octet-stream" {
		t.Fatal("incorrect upload Content-Type")
	}
	authorization, ok := strings.CutPrefix(headers.Get("Authorization"), "AWS4-HMAC-SHA256 ")
	if !ok {
		t.Fatal("request must use SigV4 Authorization")
	}
	parameters := make(map[string]string)
	for _, part := range strings.Split(authorization, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || parameters[key] != "" {
			t.Fatalf("invalid Authorization parameter %q", part)
		}
		parameters[key] = value
	}
	headerNames := strings.Split(parameters["SignedHeaders"], ";")
	if !slices.IsSorted(headerNames) {
		t.Fatal("SignedHeaders must be sorted")
	}
	for _, required := range []string{"host", "x-amz-date", "x-amz-security-token"} {
		if !slices.Contains(headerNames, required) {
			t.Fatalf("SignedHeaders does not include %s", required)
		}
	}
	var canonicalHeaders strings.Builder
	for _, name := range headerNames {
		values := headers.Values(name)
		switch name {
		case "host":
			values = []string{host}
		case "content-length":
			values = []string{strconv.Itoa(bodyLength)}
		}
		if name == "" || name != strings.ToLower(name) || len(values) == 0 {
			t.Fatalf("invalid or absent signed header %q", name)
		}
		for i, value := range values {
			values[i] = strings.Join(strings.Fields(value), " ")
		}
		fmt.Fprintf(&canonicalHeaders, "%s:%s\n", name, strings.Join(values, ","))
	}
	canonicalRequest := strings.Join([]string{http.MethodPut, uri, "", canonicalHeaders.String(), parameters["SignedHeaders"], "UNSIGNED-PAYLOAD"}, "\n")
	requestHash := sha256.Sum256([]byte(canonicalRequest))
	day := when.Format("20060102")
	scope := day + "/" + region + "/s3/aws4_request"
	if got := parameters["Credential"]; got != "test-access/"+scope {
		t.Fatalf("Credential = %q, want test-access/%s", got, scope)
	}
	toSign := "AWS4-HMAC-SHA256\n" + date + "\n" + scope + "\n" + fmt.Sprintf("%x", requestHash)
	hmacSHA256 := func(key []byte, value string) []byte {
		h := hmac.New(sha256.New, key)
		_, _ = h.Write([]byte(value))
		return h.Sum(nil)
	}
	signingKey := []byte("AWS4test-secret")
	for _, value := range []string{day, region, "s3", "aws4_request"} {
		signingKey = hmacSHA256(signingKey, value)
	}
	want := fmt.Sprintf("%x", hmacSHA256(signingKey, toSign))
	if got := parameters["Signature"]; got != want {
		t.Errorf("Signature = %q, want independently computed signature %q", got, want)
	}
}
