package loki

import (
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestValidateConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     Config
		wantErr error
	}{
		{
			name: "valid config with zero timeout sentinel",
			cfg: Config{
				Endpoint: "http://localhost:3100",
			},
		},
		{
			name: "negative timeout",
			cfg: Config{
				Endpoint: "http://localhost:3100",
				Timeout:  -time.Second,
			},
			wantErr: ErrNegativeTimeout,
		},
		{
			name: "basic auth and bearer token conflict",
			cfg: Config{
				Endpoint:    "http://localhost:3100",
				BasicAuth:   &BasicAuthConfig{Username: "user", Password: "password"},
				BearerToken: "token",
			},
			wantErr: ErrConflictingAuth,
		},
		{
			name: "invalid tenant id",
			cfg: Config{
				Endpoint: "http://localhost:3100",
				TenantID: "tenant/invalid",
			},
			wantErr: ErrInvalidTenantID,
		},
		{
			name:    "missing endpoint",
			cfg:     Config{},
			wantErr: ErrEndpointRequired,
		},
		{
			name: "invalid endpoint",
			cfg: Config{
				Endpoint: "ftp://localhost:3100",
			},
			wantErr: ErrInvalidEndpoint,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateConfig(tt.cfg)
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

func TestValidateLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		labels  map[string]string
		wantErr error
		// wantInvalidNameは、wantErrがErrInvalidLabelNameのときに報告されるlabel名です。
		wantInvalidName string
	}{
		{name: "valid", labels: map[string]string{"app_name": "service"}},
		{name: "colon is accepted", labels: map[string]string{"app:component": "service"}},
		{name: "internal-style prefix is accepted by push API", labels: map[string]string{"__app": "service"}},
		{name: "empty value is accepted", labels: map[string]string{"app": ""}},
		{name: "empty", labels: nil, wantErr: ErrLabelsRequired},
		{
			name: "starts with digit", labels: map[string]string{"1app": "service"},
			wantErr: ErrInvalidLabelName, wantInvalidName: "1app",
		},
		{
			name: "invalid character", labels: map[string]string{"app-name": "service"},
			wantErr: ErrInvalidLabelName, wantInvalidName: "app-name",
		},
	}

	for _, tt := range tests {
		testCase := tt
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateLabels(testCase.labels)
			if testCase.wantErr == nil {
				if err != nil {
					t.Fatalf("ValidateLabels() error = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("ValidateLabels() error = %v, want %v", err, testCase.wantErr)
			}
			if testCase.wantInvalidName == "" {
				return
			}
			var nameErr *InvalidLabelNameError
			if !errors.As(err, &nameErr) || nameErr.Name != testCase.wantInvalidName {
				t.Fatalf("ValidateLabels() error = %v, want InvalidLabelNameError{Name: %q}", err, testCase.wantInvalidName)
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
		"https://" + user + ":" + password + "@loki.example.test/%zz",
		// url.Parseは"/"でauthorityを区切るため、パスワードの一部をportとして報告します。
		"https://" + user + ":S3cret/Passw0rd@loki.example.test/",
	} {
		err := ValidateConfig(Config{Endpoint: endpoint})
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
