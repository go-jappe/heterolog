package s3

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestValidateConfig(t *testing.T) {
	t.Parallel()

	valid := validS3ConfigForValidation()
	tests := []struct {
		name   string
		mutate func(*objectstorage.S3Config)
		// wantIsは、呼び出し側がerrors.Isで判別する設定errorです。nilなら成功を求めます。
		wantIs error
	}{
		{name: "valid default endpoint"},
		{
			name: "zero timeout uses client default",
			mutate: func(cfg *objectstorage.S3Config) {
				cfg.Timeout = 0
			},
		},
		{
			name: "negative timeout",
			mutate: func(cfg *objectstorage.S3Config) {
				cfg.Timeout = -time.Second
			},
			wantIs: ErrNegativeTimeout,
		},
		{
			name: "region is normalized before validation",
			mutate: func(cfg *objectstorage.S3Config) {
				cfg.Region = " AP-NORTHEAST-1 "
			},
		},
		{
			name: "missing region",
			mutate: func(cfg *objectstorage.S3Config) {
				cfg.Region = " "
			},
			wantIs: ErrRegionRequired,
		},
		{
			name: "missing bucket",
			mutate: func(cfg *objectstorage.S3Config) {
				cfg.Bucket = " "
			},
			wantIs: ErrBucketRequired,
		},
		{
			name: "bucket contains path separator",
			mutate: func(cfg *objectstorage.S3Config) {
				cfg.Bucket = "archive/bucket"
			},
			wantIs: ErrBucketContainsPathSeparator,
		},
		{
			name: "missing access key",
			mutate: func(cfg *objectstorage.S3Config) {
				cfg.AccessKeyID = ""
			},
			wantIs: ErrAccessKeyIDRequired,
		},
		{
			name: "missing secret key",
			mutate: func(cfg *objectstorage.S3Config) {
				cfg.SecretAccessKey = ""
			},
			wantIs: ErrSecretAccessKeyRequired,
		},
		{
			name: "endpoint lacks scheme",
			mutate: func(cfg *objectstorage.S3Config) {
				cfg.Endpoint = "s3.local"
			},
			wantIs: ErrInvalidEndpoint,
		},
		{
			// schemeはhttpsなので、host欠落の検査だけが拒否理由になります。
			name: "endpoint lacks host",
			mutate: func(cfg *objectstorage.S3Config) {
				cfg.Endpoint = "https:///archive"
			},
			wantIs: ErrInvalidEndpoint,
		},
		{
			name: "endpoint contains user info",
			mutate: func(cfg *objectstorage.S3Config) {
				cfg.Endpoint = "https://user:password@s3.local"
			},
			wantIs: ErrInvalidEndpoint,
		},
		{
			name: "endpoint uses unsupported scheme",
			mutate: func(cfg *objectstorage.S3Config) {
				cfg.Endpoint = "ftp://s3.local"
			},
			wantIs: ErrInvalidEndpoint,
		},
		{
			// queryはobject URLを経由してすべてのPUTへ付与されてしまいます。
			name: "endpoint contains query",
			mutate: func(cfg *objectstorage.S3Config) {
				cfg.Endpoint = "https://s3.local/?b=2&a=1"
			},
			wantIs: ErrInvalidEndpoint,
		},
		{
			name: "endpoint contains empty query",
			mutate: func(cfg *objectstorage.S3Config) {
				cfg.Endpoint = "https://s3.local/proxy?"
			},
			wantIs: ErrInvalidEndpoint,
		},
		{
			name: "endpoint contains fragment",
			mutate: func(cfg *objectstorage.S3Config) {
				cfg.Endpoint = "https://s3.local/proxy#archive"
			},
			wantIs: ErrInvalidEndpoint,
		},
		{
			// エスケープされた?と#はproxy path segmentのbyteであり、queryやfragmentではありません。
			name: "endpoint path contains escaped query and fragment delimiters",
			mutate: func(cfg *objectstorage.S3Config) {
				cfg.Endpoint = "https://s3.local/proxy%3Fa%23b/"
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := valid
			if tt.mutate != nil {
				tt.mutate(&cfg)
			}
			err := ValidateConfig(cfg)
			if tt.wantIs == nil {
				if err != nil {
					t.Fatalf("ValidateConfig() error = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantIs) {
				t.Fatalf("ValidateConfig() error = %v, want errors.Is(..., %v)", err, tt.wantIs)
			}
		})
	}
}

func TestNewUsesDefaultTimeoutForZeroConfig(t *testing.T) {
	t.Parallel()

	client, err := New(Config{S3Config: objectstorage.S3Config{
		Region:          "ap-northeast-1",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		Bucket:          "archive-bucket",
	}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if client.httpClient.Timeout != defaultTimeout {
		t.Fatalf("New() http client timeout = %s, want %s", client.httpClient.Timeout, defaultTimeout)
	}
}

// endpoint未指定では、前後の空白を除いたregionからAWS S3のdefault endpointを組み立て、
// 同じregionでSigV4署名します。送信されたrequestのhostとcredential scopeで確認します。
func TestNewUsesTrimmedRegionForDefaultEndpointAndSigning(t *testing.T) {
	t.Parallel()

	filePath := filepath.Join(t.TempDir(), "archive.gz")
	if err := os.WriteFile(filePath, []byte("archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	var gotHost, gotAuthorization string
	client, err := New(Config{
		S3Config: objectstorage.S3Config{
			Region:          " ap-northeast-1 ",
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
			Bucket:          "archive-bucket",
		},
		HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			gotHost, gotAuthorization = r.URL.Host, r.Header.Get("Authorization")
			_, _ = io.Copy(io.Discard, r.Body)
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Header: make(http.Header), Request: r}, nil
		})},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := client.Upload(context.Background(), uploadertest.Input(t, filePath, "archive.gz")); err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	if gotHost != "archive-bucket.s3.ap-northeast-1.amazonaws.com" {
		t.Fatalf("request host = %q, want virtual-hosted default endpoint for the trimmed region", gotHost)
	}
	if !strings.Contains(gotAuthorization, "/ap-northeast-1/s3/aws4_request") {
		t.Fatalf("Authorization = %q, want credential scope for the trimmed region", gotAuthorization)
	}
}

func validS3ConfigForValidation() objectstorage.S3Config {
	return objectstorage.S3Config{
		Region:          "ap-northeast-1",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		Bucket:          "archive-bucket",
		Timeout:         time.Second,
	}
}

// endpointのURLにuser info（ユーザー名・パスワード）があり解析できない場合も、errorの文面と、原因として
// 残すnet/urlの*url.Errorにuser infoを含めません。宛先IDの生成も同じ検証を通ります。資格情報を
// 文面へ出さないことが要件なので、文面を直接確認します。
func TestValidateConfigDoesNotExposeEndpointUserInfo(t *testing.T) {
	t.Parallel()

	const user, password = "endpoint-user", "S3cret-Passw0rd"
	for _, endpoint := range []string{
		// pathのescapeが不正です。
		"https://" + user + ":" + password + "@s3.example.test/%zz",
		// url.Parseは"/"でauthorityを区切るため、パスワードの一部をportとして報告します。
		"https://" + user + ":S3cret/Passw0rd@s3.example.test/",
	} {
		cfg := validS3ConfigForValidation()
		cfg.Endpoint = endpoint
		_, resolveErr := ResolveDestinationID(cfg)
		for name, err := range map[string]error{"ValidateConfig": ValidateConfig(cfg), "ResolveDestinationID": resolveErr} {
			var urlErr *url.Error
			if !errors.Is(err, ErrInvalidEndpoint) || !errors.As(err, &urlErr) {
				t.Fatalf("%s() error = %v, want ErrInvalidEndpoint caused by the URL parse error", name, err)
			}
			for _, secret := range []string{user, "S3cret", "Passw0rd"} {
				if strings.Contains(err.Error(), secret) || strings.Contains(urlErr.URL, secret) {
					t.Fatalf("%s() error %q exposes %q from the endpoint user info", name, err, secret)
				}
			}
		}
	}
}
