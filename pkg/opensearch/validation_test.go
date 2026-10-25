package opensearch

import (
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestValidateConfigRejectsNegativeTimeout(t *testing.T) {
	err := ValidateConfig(Config{
		Endpoint:   "http://localhost:9200",
		Timeout:    -time.Second,
		AuthConfig: AuthConfig{Type: AuthTypeNone},
	})
	if !errors.Is(err, ErrNegativeTimeout) {
		t.Fatalf("ValidateConfig() error = %v, want %v", err, ErrNegativeTimeout)
	}
}

func TestValidateIndex(t *testing.T) {
	tests := []struct {
		name    string
		index   string
		wantErr error
	}{
		{name: "valid", index: "application-logs"},
		{name: "empty", wantErr: ErrIndexRequired},
		{name: "blank", index: " ", wantErr: ErrIndexRequired},
		{name: "surrounding whitespace", index: " application-logs ", wantErr: ErrIndexHasSurroundingWhitespace},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateIndex(tt.index)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("ValidateIndex(%q) error = %v", tt.index, err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ValidateIndex(%q) error = %v, want %v", tt.index, err, tt.wantErr)
			}
		})
	}
}

func TestValidateConfigAcceptsSupportedAuthConfigurations(t *testing.T) {
	tests := []struct {
		name       string
		endpoint   string
		authConfig AuthConfig
	}{
		{
			name:     "none",
			endpoint: "http://localhost:9200",
			authConfig: AuthConfig{
				Type: AuthTypeNone,
			},
		},
		{
			name:     "basic",
			endpoint: "http://localhost:9200/base",
			authConfig: AuthConfig{
				Type: AuthTypeBasic,
				Config: BasicAuthConfig{
					Username: "admin",
					Password: "password",
				},
			},
		},
		{
			name:     "api key",
			endpoint: "https://search.example.com",
			authConfig: AuthConfig{
				Type: AuthTypeAPIKey,
				Config: APIKeyAuthConfig{
					ID:     "id",
					Secret: "secret",
				},
			},
		},
		{
			name:     "aws iam",
			endpoint: "https://search.example.com",
			authConfig: AuthConfig{
				Type: AuthTypeAWSIAM,
				Config: AWSIAMAuthConfig{
					Region:          "ap-northeast-1",
					AccessKeyID:     "access-key",
					SecretAccessKey: "secret-key",
				},
			},
		},
		{
			name:     "saml",
			endpoint: "https://search.example.com",
			authConfig: AuthConfig{
				Type:   AuthTypeSAML,
				Config: SAMLAuthConfig{Token: "token"},
			},
		},
		{
			name:     "oidc",
			endpoint: "https://search.example.com",
			authConfig: AuthConfig{
				Type:   AuthTypeOIDC,
				Config: OIDCAuthConfig{Token: "token"},
			},
		},
		{
			name:     "certificate",
			endpoint: "https://search.example.com",
			authConfig: AuthConfig{
				Type: AuthTypeCertificate,
				Config: CertificateAuthConfig{
					CertFile: "/not-read/client.crt",
					KeyFile:  "/not-read/client.key",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfig(Config{
				Endpoint:   tt.endpoint,
				AuthConfig: tt.authConfig,
			})
			if err != nil {
				t.Fatalf("ValidateConfig() error = %v", err)
			}
		})
	}
}

func TestValidateConfigRejectsInvalidEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		want     error
	}{
		{name: "empty", endpoint: "", want: ErrEndpointRequired},
		{name: "surrounding whitespace", endpoint: " http://localhost:9200", want: ErrEndpointHasSurroundingWhitespace},
		{name: "missing scheme", endpoint: "localhost:9200", want: ErrInvalidEndpoint},
		{name: "unsupported scheme", endpoint: "ftp://localhost:9200", want: ErrInvalidEndpoint},
		{name: "missing host", endpoint: "http:///logs", want: ErrInvalidEndpoint},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfig(Config{
				Endpoint: tt.endpoint,
				AuthConfig: AuthConfig{
					Type: AuthTypeNone,
				},
			})
			if !errors.Is(err, tt.want) {
				t.Fatalf("ValidateConfig() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestValidateConfigRejectsInvalidAuthConfiguration(t *testing.T) {
	tests := []struct {
		name       string
		endpoint   string
		authConfig AuthConfig
		// wantIsは、呼び出し側がerrors.Isで判別する設定errorです。
		wantIs error
	}{
		{
			name:       "unsupported auth type",
			authConfig: AuthConfig{Type: AuthType("unknown")},
			wantIs:     ErrUnsupportedAuthType,
		},
		{
			name:       "basic config type",
			authConfig: AuthConfig{Type: AuthTypeBasic, Config: APIKeyAuthConfig{}},
			wantIs:     ErrInvalidAuthConfigType,
		},
		{
			name:       "basic username",
			authConfig: AuthConfig{Type: AuthTypeBasic, Config: BasicAuthConfig{Password: "password"}},
			wantIs:     ErrBasicAuthUsernameRequired,
		},
		{
			name:       "basic password",
			authConfig: AuthConfig{Type: AuthTypeBasic, Config: BasicAuthConfig{Username: "admin"}},
			wantIs:     ErrBasicAuthPasswordRequired,
		},
		{
			// 空白だけの認証情報も未設定として扱い、basic認証の設定エラーにします。
			name:       "blank basic username",
			authConfig: AuthConfig{Type: AuthTypeBasic, Config: BasicAuthConfig{Username: " \t", Password: "password"}},
			wantIs:     ErrBasicAuthUsernameRequired,
		},
		{
			name:       "blank basic password",
			authConfig: AuthConfig{Type: AuthTypeBasic, Config: BasicAuthConfig{Username: "user", Password: "  "}},
			wantIs:     ErrBasicAuthPasswordRequired,
		},
		{
			name:       "empty basic credentials",
			authConfig: AuthConfig{Type: AuthTypeBasic, Config: BasicAuthConfig{}},
			wantIs:     ErrBasicAuthUsernameRequired,
		},
		{
			name:       "api key config type",
			authConfig: AuthConfig{Type: AuthTypeAPIKey, Config: BasicAuthConfig{}},
			wantIs:     ErrInvalidAuthConfigType,
		},
		{
			name:       "api key id",
			authConfig: AuthConfig{Type: AuthTypeAPIKey, Config: APIKeyAuthConfig{Secret: "secret"}},
			wantIs:     ErrAPIKeyIDRequired,
		},
		{
			name:       "api key secret",
			authConfig: AuthConfig{Type: AuthTypeAPIKey, Config: APIKeyAuthConfig{ID: "id"}},
			wantIs:     ErrAPIKeySecretRequired,
		},
		{
			name:       "aws iam config type",
			authConfig: AuthConfig{Type: AuthTypeAWSIAM, Config: BasicAuthConfig{}},
			wantIs:     ErrInvalidAuthConfigType,
		},
		{
			name: "aws iam region",
			authConfig: AuthConfig{Type: AuthTypeAWSIAM, Config: AWSIAMAuthConfig{
				AccessKeyID:     "access-key",
				SecretAccessKey: "secret-key",
			}},
			wantIs: ErrAWSIAMRegionRequired,
		},
		{
			name: "aws iam access key",
			authConfig: AuthConfig{Type: AuthTypeAWSIAM, Config: AWSIAMAuthConfig{
				Region:          "ap-northeast-1",
				SecretAccessKey: "secret-key",
			}},
			wantIs: ErrAWSIAMAccessKeyIDRequired,
		},
		{
			name: "aws iam secret key",
			authConfig: AuthConfig{Type: AuthTypeAWSIAM, Config: AWSIAMAuthConfig{
				Region:      "ap-northeast-1",
				AccessKeyID: "access-key",
			}},
			wantIs: ErrAWSIAMSecretAccessKeyRequired,
		},
		{
			name:       "saml config type",
			authConfig: AuthConfig{Type: AuthTypeSAML, Config: OIDCAuthConfig{}},
			wantIs:     ErrInvalidAuthConfigType,
		},
		{
			name:       "saml token",
			authConfig: AuthConfig{Type: AuthTypeSAML, Config: SAMLAuthConfig{}},
			wantIs:     ErrSAMLTokenRequired,
		},
		{
			name:       "oidc config type",
			authConfig: AuthConfig{Type: AuthTypeOIDC, Config: SAMLAuthConfig{}},
			wantIs:     ErrInvalidAuthConfigType,
		},
		{
			name:       "oidc token",
			authConfig: AuthConfig{Type: AuthTypeOIDC, Config: OIDCAuthConfig{}},
			wantIs:     ErrOIDCTokenRequired,
		},
		{
			name:       "certificate config type",
			authConfig: AuthConfig{Type: AuthTypeCertificate, Config: BasicAuthConfig{}},
			wantIs:     ErrInvalidAuthConfigType,
		},
		{
			name:     "certificate requires https",
			endpoint: "http://localhost:9200",
			authConfig: AuthConfig{Type: AuthTypeCertificate, Config: CertificateAuthConfig{
				CertFile: "client.crt",
				KeyFile:  "client.key",
			}},
			wantIs: ErrCertificateAuthRequiresHTTPS,
		},
		{
			name:     "certificate cert file",
			endpoint: "https://localhost:9200",
			authConfig: AuthConfig{Type: AuthTypeCertificate, Config: CertificateAuthConfig{
				KeyFile: "client.key",
			}},
			wantIs: ErrCertificateFileRequired,
		},
		{
			name:     "certificate key file",
			endpoint: "https://localhost:9200",
			authConfig: AuthConfig{Type: AuthTypeCertificate, Config: CertificateAuthConfig{
				CertFile: "client.crt",
			}},
			wantIs: ErrCertificateKeyFileRequired,
		},
		{
			name:       "none config",
			authConfig: AuthConfig{Type: AuthTypeNone, Config: BasicAuthConfig{}},
			wantIs:     ErrAuthConfigNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			endpoint := tt.endpoint
			if endpoint == "" {
				endpoint = "https://localhost:9200"
			}
			err := ValidateConfig(Config{
				Endpoint:   endpoint,
				AuthConfig: tt.authConfig,
			})
			if !errors.Is(err, tt.wantIs) {
				t.Fatalf("ValidateConfig() error = %v, want errors.Is(..., %v)", err, tt.wantIs)
			}
		})
	}
}

func TestNewRejectsInvalidConfigBeforeCreatingClient(t *testing.T) {
	_, err := New(Config{
		Endpoint: "localhost:9200",
		AuthConfig: AuthConfig{
			Type: AuthTypeNone,
		},
	})
	if !errors.Is(err, ErrInvalidEndpoint) {
		t.Fatalf("New() error = %v, want %v", err, ErrInvalidEndpoint)
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
		"https://" + user + ":" + password + "@opensearch.example.test:9200/%zz",
		// url.Parseは"/"でauthorityを区切るため、パスワードの一部をportとして報告します。
		"https://" + user + ":S3cret/Passw0rd@opensearch.example.test:9200/",
	} {
		err := ValidateConfig(Config{Endpoint: endpoint, AuthConfig: AuthConfig{Type: AuthTypeNone}})
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
