package cloudwatchlogs

import (
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestValidateConfig(t *testing.T) {
	t.Parallel()

	valid := Config{
		Region:          "ap-northeast-1",
		LogGroupName:    "/app/test",
		AccessKeyID:     "access-key",
		SecretAccessKey: "secret-key",
	}

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr error
	}{
		{name: "valid config"},
		{
			name: "negative timeout",
			mutate: func(cfg *Config) {
				cfg.Timeout = -time.Second
			},
			wantErr: ErrNegativeTimeout,
		},
		{
			name: "missing log group",
			mutate: func(cfg *Config) {
				cfg.LogGroupName = " "
			},
			wantErr: ErrLogGroupNameRequired,
		},
		{
			name: "missing region",
			mutate: func(cfg *Config) {
				cfg.Region = " "
			},
			wantErr: ErrRegionRequired,
		},
		{
			name: "endpoint uses unsupported scheme",
			mutate: func(cfg *Config) {
				cfg.Endpoint = "ftp://localhost:4566"
			},
			wantErr: ErrInvalidEndpoint,
		},
		{
			// schemeはhttpsなので、host欠落の検査だけが拒否理由になります。
			name: "endpoint lacks host",
			mutate: func(cfg *Config) {
				cfg.Endpoint = "https:///logs"
			},
			wantErr: ErrInvalidEndpoint,
		},
		{
			name: "endpoint contains user info",
			mutate: func(cfg *Config) {
				cfg.Endpoint = "https://user:password@logs.example.test"
			},
			wantErr: ErrInvalidEndpoint,
		},
		{
			name: "endpoint contains query",
			mutate: func(cfg *Config) {
				cfg.Endpoint = "https://logs.example.test/?b=2&a=1"
			},
			wantErr: ErrInvalidEndpoint,
		},
		{
			name: "endpoint contains empty query",
			mutate: func(cfg *Config) {
				cfg.Endpoint = "https://logs.example.test/proxy?"
			},
			wantErr: ErrInvalidEndpoint,
		},
		{
			name: "endpoint contains fragment",
			mutate: func(cfg *Config) {
				cfg.Endpoint = "https://logs.example.test/proxy#logs"
			},
			wantErr: ErrInvalidEndpoint,
		},
		{
			// エスケープされた?と#はproxy path segmentのbyteであり、queryやfragmentではありません。
			name: "endpoint path contains escaped query and fragment delimiters",
			mutate: func(cfg *Config) {
				cfg.Endpoint = "https://logs.example.test/proxy%3Fa%23b"
			},
		},
		{
			name: "missing access key",
			mutate: func(cfg *Config) {
				cfg.AccessKeyID = ""
			},
			wantErr: ErrCredentialsRequired,
		},
		{
			name: "missing secret key",
			mutate: func(cfg *Config) {
				cfg.SecretAccessKey = ""
			},
			wantErr: ErrSecretAccessKeyRequired,
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
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("ValidateConfig() error = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ValidateConfig() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateConfigAcceptsCredentialsProviderWithoutRetrievingCredentials(t *testing.T) {
	t.Parallel()

	provider := &recordingCredentialsProvider{
		credentials: aws.Credentials{AccessKeyID: "provider-access-key"},
	}
	err := ValidateConfig(Config{
		Region:              "ap-northeast-1",
		LogGroupName:        "/app/test",
		CredentialsProvider: provider,
	})
	if err != nil {
		t.Fatalf("ValidateConfig() error = %v, want nil", err)
	}
	if provider.calls != 0 {
		t.Fatalf("ValidateConfig() credentials provider calls = %d, want 0", provider.calls)
	}
}

func TestValidateLogStreamName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		streamName string
		wantErr    error
	}{
		{name: "valid", streamName: "app/task-1"},
		{name: "empty", streamName: "", wantErr: ErrLogStreamNameRequired},
		{name: "whitespace is allowed by AWS", streamName: " "},
		{name: "forbidden colon", streamName: "app:task", wantErr: ErrLogStreamNameInvalidCharacter},
		{name: "forbidden asterisk", streamName: "app*", wantErr: ErrLogStreamNameInvalidCharacter},
		{
			name:       "too many characters",
			streamName: strings.Repeat("a", maxLogStreamNameLength+1),
			wantErr:    ErrLogStreamNameTooLong,
		},
		{
			name:       "unicode limit counts characters instead of UTF-8 bytes",
			streamName: strings.Repeat("あ", maxLogStreamNameLength),
		},
		{
			name:       "unicode characters above the limit",
			streamName: strings.Repeat("あ", maxLogStreamNameLength+1),
			wantErr:    ErrLogStreamNameTooLong,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateLogStreamName(tt.streamName)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("ValidateLogStreamName() error = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ValidateLogStreamName() error = %v, want errors.Is(..., %v)", err, tt.wantErr)
			}
		})
	}
}

// endpointのURLにuser info（ユーザー名・パスワード）があり解析できない場合も、errorの文面と、原因として
// 残すnet/urlの*url.Errorにuser infoを含めません。資格情報を文面へ出さないことが要件なので、
// 文面を直接確認します。
func TestValidateConfigDoesNotExposeEndpointUserInfo(t *testing.T) {
	t.Parallel()

	const user, password = "endpoint-user", "S3cret-Passw0rd"
	for _, endpoint := range []string{
		// pathのescapeが不正です。
		"https://" + user + ":" + password + "@logs.example.test/%zz",
		// url.Parseは"/"でauthorityを区切るため、パスワードの一部をportとして報告します。
		"https://" + user + ":S3cret/Passw0rd@logs.example.test/",
	} {
		err := ValidateConfig(Config{
			Region:          "ap-northeast-1",
			LogGroupName:    "/app/test",
			AccessKeyID:     "access-key",
			SecretAccessKey: "secret-key",
			Endpoint:        endpoint,
		})
		var urlErr *url.Error
		if !errors.Is(err, ErrInvalidEndpoint) || !errors.As(err, &urlErr) {
			t.Fatalf("ValidateConfig() error = %v, want ErrInvalidEndpoint caused by the URL parse error", err)
		}
		for _, secret := range []string{user, "S3cret", "Passw0rd"} {
			if strings.Contains(err.Error(), secret) || strings.Contains(urlErr.URL, secret) {
				t.Fatalf("ValidateConfig() error %q exposes %q from the endpoint user info", err, secret)
			}
		}
	}
}
