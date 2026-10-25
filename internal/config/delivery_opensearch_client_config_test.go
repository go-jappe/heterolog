package config

import (
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	configopensearch "github.com/go-jappe/heterolog/internal/config/opensearch"
	"github.com/go-jappe/heterolog/pkg/env"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/stretchr/testify/require"
)

func TestOpenSearchClientEnvironmentKeepsEveryRoleAndAuthIndependent(t *testing.T) {
	for _, auth := range []struct {
		name string
		want func(string) any
	}{
		{"basic", func(role string) any {
			return opensearch.BasicAuthConfig{Username: role + "-user", Password: role + "-password"}
		}},
		{"apikey", func(role string) any {
			return opensearch.APIKeyAuthConfig{ID: role + "-id", Secret: role + "-secret"}
		}},
		{"awsiam", func(role string) any {
			return opensearch.AWSIAMAuthConfig{
				Region: role + "-region", AccessKeyID: role + "-access",
				SecretAccessKey: role + "-secret-access", SessionToken: role + "-session",
			}
		}},
		{"saml", func(role string) any { return opensearch.SAMLAuthConfig{Token: role + "-saml"} }},
		{"oidc", func(role string) any { return opensearch.OIDCAuthConfig{Token: role + "-oidc"} }},
		{"certificate", func(role string) any {
			return opensearch.CertificateAuthConfig{
				CertFile: role + "-cert.pem", KeyFile: role + "-key.pem", CAFile: role + "-ca.pem",
			}
		}},
		{"none", func(string) any { return nil }},
	} {
		t.Run(auth.name, func(t *testing.T) {
			restoreProcessEnvAfterTest(t)
			clearProcessEnvForTest(t)
			roles := []struct {
				name, prefix string
				load         func() (configopensearch.OpenSearchClientConfig, error)
			}{
				{"sync", "", fromProcessEnvironment(loadOpenSearchClientConfig)},
				{"async", "ASYNC_", fromProcessEnvironment(loadAsyncOpenSearchClientConfig)},
				{"delivery", "DELIVERY_", fromProcessEnvironment(loadDeliveryOpenSearchClientConfig)},
			}
			// timeoutは役割ごとに異なり、既定値（1秒）とも異なる値にします。既定値と同じ値では、
			// keyを読まずに既定値を使う実装も期待値と一致するためです。
			roleTimeout := func(index int) time.Duration { return time.Duration(index+2) * time.Second }
			for index, role := range roles {
				if roleTimeout(index) == configopensearch.DefaultOpenSearchTimeout {
					t.Fatalf("%s timeout %s equals the default; choose a different value", role.name, roleTimeout(index))
				}
				for suffix, value := range map[string]string{
					"ENDPOINT":              "https://" + role.name + ".example:9200",
					"TIMEOUT_SECOND":        strconv.Itoa(int(roleTimeout(index) / time.Second)),
					"SKIP_VERIFY_SSL":       strconv.FormatBool(index != 1),
					"AUTH_TYPE":             auth.name,
					"USERNAME":              role.name + "-user",
					"PASSWORD":              role.name + "-password",
					"API_KEY_ID":            role.name + "-id",
					"API_KEY_SECRET":        role.name + "-secret",
					"AWS_REGION":            role.name + "-region",
					"AWS_ACCESS_KEY_ID":     role.name + "-access",
					"AWS_SECRET_ACCESS_KEY": role.name + "-secret-access",
					"AWS_SESSION_TOKEN":     role.name + "-session",
					"SAML_TOKEN":            role.name + "-saml",
					"OIDC_TOKEN":            role.name + "-oidc",
					"CERT_FILE":             role.name + "-cert.pem",
					"KEY_FILE":              role.name + "-key.pem",
					"CA_FILE":               role.name + "-ca.pem",
				} {
					t.Setenv("HETEROLOG_"+role.prefix+"OPENSEARCH_"+suffix, value)
				}
				t.Setenv("HETEROLOG_ENABLE_"+role.prefix+"OPENSEARCH_COMPRESS_REQUEST", strconv.FormatBool(index == 1))
			}
			for index, role := range roles {
				cfg, err := role.load()
				require.NoError(t, err)
				mapped := newOpenSearchClientConfig(cfg)
				require.Equal(t, &opensearch.Config{
					Endpoint: "https://" + role.name + ".example:9200", Timeout: roleTimeout(index),
					SkipVerifySSL: index != 1, CompressRequest: index == 1,
					AuthConfig: opensearch.AuthConfig{Type: opensearch.AuthType(auth.name), Config: auth.want(role.name)},
				}, mapped, role.name)
				require.NoError(t, opensearch.ValidateConfig(*mapped), role.name)
			}
		})
	}
}

func TestDeliveryOpenSearchClientDefaultsDoNotInheritAsync(t *testing.T) {
	// sync/asyncへ与える値は、Deliveryの既定値と異なる値にします。既定値と同じ値では、
	// Deliveryが継承しても期待値と一致し、継承を検出できないためです。
	const otherTimeout = 7 * time.Second
	if otherTimeout == configopensearch.DefaultOpenSearchTimeout {
		t.Fatalf("sync/async timeout %s equals the Delivery default; choose a different value", otherTimeout)
	}
	for _, empty := range []bool{false, true} {
		t.Run(strconv.FormatBool(empty), func(t *testing.T) {
			restoreProcessEnvAfterTest(t)
			clearProcessEnvForTest(t)
			for _, prefix := range []string{"", "ASYNC_"} {
				t.Setenv("HETEROLOG_"+prefix+"OPENSEARCH_ENDPOINT", "https://other.example")
				t.Setenv("HETEROLOG_"+prefix+"OPENSEARCH_TIMEOUT_SECOND", strconv.Itoa(int(otherTimeout/time.Second)))
				t.Setenv("HETEROLOG_"+prefix+"OPENSEARCH_SKIP_VERIFY_SSL", "true")
				t.Setenv("HETEROLOG_"+prefix+"OPENSEARCH_AUTH_TYPE", "none")
				t.Setenv("HETEROLOG_"+prefix+"OPENSEARCH_USERNAME", "other-user")
				t.Setenv("HETEROLOG_"+prefix+"OPENSEARCH_PASSWORD", "other-password")
				t.Setenv("HETEROLOG_ENABLE_"+prefix+"OPENSEARCH_COMPRESS_REQUEST", "false")
			}
			if empty {
				for _, suffix := range []string{"ENDPOINT", "TIMEOUT_SECOND", "SKIP_VERIFY_SSL", "AUTH_TYPE", "USERNAME", "PASSWORD"} {
					t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_"+suffix, "")
				}
				t.Setenv("HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_COMPRESS_REQUEST", "")
			}
			cfg, err := loadDeliveryOpenSearchClientConfig(processEnvironmentForTest())
			require.NoError(t, err)
			// 認証情報はsync/asyncから継承せず、固定値でも補完しません。
			require.Equal(t, configopensearch.OpenSearchClientConfig{
				Endpoint: configopensearch.DefaultOpenSearchEndpoint, Timeout: configopensearch.DefaultOpenSearchTimeout,
				SkipVerifySSL: configopensearch.DefaultOpenSearchSkipVerify, CompressRequest: configopensearch.DefaultCompressRequest,
				Auth: configopensearch.OpenSearchAuthConfig{
					Type:  configopensearch.DefaultAuthType,
					Basic: &configopensearch.BasicAuthConfig{},
				},
			}, cfg)
			require.ErrorIs(t, opensearch.ValidateConfig(*newOpenSearchClientConfig(cfg)),
				opensearch.ErrBasicAuthUsernameRequired)
		})
	}
}

// TestOpenSearchBasicAuthRequiresExplicitCredentialsは、basic認証の利用者名・passwordを
// 暗黙に補完せず、OpenSearchを使うLoggerContractKindでは不足を設定エラーにすることを
// 検証します。公開例と同じく空の認証情報でも、OpenSearchを使わない構成は起動できます。
func TestOpenSearchBasicAuthRequiresExplicitCredentials(t *testing.T) {
	contracts := []struct {
		name, prefix, enableKey string
		loaded                  func(AppConfig) configopensearch.OpenSearchClientConfig
		mapConfig               func(AppConfig) error
	}{
		{
			name: "sync", prefix: "", enableKey: "HETEROLOG_ENABLE_LOGGING",
			loaded: func(cfg AppConfig) configopensearch.OpenSearchClientConfig { return cfg.OpenSearchClient },
			mapConfig: func(cfg AppConfig) error {
				_, err := MapLoggerConfig(cfg)
				return err
			},
		},
		{
			name: "async", prefix: "ASYNC_", enableKey: "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING",
			loaded: func(cfg AppConfig) configopensearch.OpenSearchClientConfig { return cfg.AsyncOpenSearchClient },
			mapConfig: func(cfg AppConfig) error {
				_, err := MapAsyncLoggerConfig(cfg)
				return err
			},
		},
		{
			name: "delivery", prefix: "DELIVERY_", enableKey: "HETEROLOG_ENABLE_DELIVERY_LOGGING",
			loaded: func(cfg AppConfig) configopensearch.OpenSearchClientConfig { return cfg.DeliveryOpenSearchClient },
			mapConfig: func(cfg AppConfig) error {
				_, err := MapDeliveryLoggerConfig(cfg)
				return err
			},
		},
	}
	tests := []struct {
		name      string
		selection []string
		auth      []string
		wantErr   error
		wantBasic configopensearch.BasicAuthConfig
	}{
		{
			name:      "default basic auth without credentials",
			selection: []string{"HETEROLOG_LOG_TYPE=opensearch"},
			wantErr:   opensearch.ErrBasicAuthUsernameRequired,
		},
		{
			name:      "public example empty credentials",
			selection: []string{"HETEROLOG_LOG_TYPE=opensearch"},
			auth:      []string{"HETEROLOG_OPENSEARCH_AUTH_TYPE=basic", "HETEROLOG_OPENSEARCH_USERNAME=", "HETEROLOG_OPENSEARCH_PASSWORD="},
			wantErr:   opensearch.ErrBasicAuthUsernameRequired,
		},
		{
			name:      "missing password",
			selection: []string{"HETEROLOG_LOG_TYPE=opensearch"},
			auth:      []string{"HETEROLOG_OPENSEARCH_USERNAME=search-user"},
			wantErr:   opensearch.ErrBasicAuthPasswordRequired,
			wantBasic: configopensearch.BasicAuthConfig{Username: "search-user"},
		},
		{
			name:      "blank username",
			selection: []string{"HETEROLOG_LOG_TYPE=opensearch"},
			auth:      []string{`HETEROLOG_OPENSEARCH_USERNAME="   "`, "HETEROLOG_OPENSEARCH_PASSWORD=search-password"},
			wantErr:   opensearch.ErrBasicAuthUsernameRequired,
			wantBasic: configopensearch.BasicAuthConfig{Username: "   ", Password: "search-password"},
		},
		{
			name:      "internal log handler target without credentials",
			selection: []string{"HETEROLOG_LOG_TYPE=stdout", "HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER=opensearch"},
			wantErr:   opensearch.ErrBasicAuthUsernameRequired,
		},
		{
			name:      "explicit credentials",
			selection: []string{"HETEROLOG_LOG_TYPE=opensearch"},
			auth:      []string{"HETEROLOG_OPENSEARCH_USERNAME=search-user", "HETEROLOG_OPENSEARCH_PASSWORD=search-password"},
			wantBasic: configopensearch.BasicAuthConfig{Username: "search-user", Password: "search-password"},
		},
	}
	for _, contract := range contracts {
		for _, tt := range tests {
			t.Run(contract.name+"/"+tt.name, func(t *testing.T) {
				lines := []string{
					contract.enableKey + "=true",
					"HETEROLOG_" + contract.prefix + "OPENSEARCH_ENDPOINT=https://" + contract.name + ".example:9200",
				}
				for _, line := range append(append([]string{}, tt.selection...), tt.auth...) {
					lines = append(lines, "HETEROLOG_"+contract.prefix+strings.TrimPrefix(line, "HETEROLOG_"))
				}
				cfg, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t, lines...))
				require.NoError(t, err)

				client := contract.loaded(cfg)
				require.Equal(t, opensearch.AuthTypeBasic, client.Auth.Type)
				require.NotNil(t, client.Auth.Basic)
				require.Equal(t, tt.wantBasic, *client.Auth.Basic)

				err = contract.mapConfig(cfg)
				if tt.wantErr == nil {
					require.NoError(t, err)
					return
				}
				require.ErrorIs(t, err, tt.wantErr)
			})
		}

		t.Run(contract.name+"/unused client with empty credentials", func(t *testing.T) {
			// 公開例はHETEROLOG_ENABLE_OPENSEARCHを有効にしたまま認証情報を空にしています。
			// OpenSearchを出力先に選ばない限り、空の認証情報は起動を妨げません。
			cfg, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t,
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_ENABLE_OPENSEARCH=true",
				"HETEROLOG_ENABLE_ASYNC_OPENSEARCH=true",
				"HETEROLOG_LOG_TYPE=stdout",
				"HETEROLOG_ASYNC_LOG_TYPE=stdout",
				"HETEROLOG_DELIVERY_LOG_TYPE=stdout",
				"HETEROLOG_"+contract.prefix+"OPENSEARCH_AUTH_TYPE=basic",
				"HETEROLOG_"+contract.prefix+"OPENSEARCH_USERNAME=",
				"HETEROLOG_"+contract.prefix+"OPENSEARCH_PASSWORD=",
			))
			require.NoError(t, err)
			require.NoError(t, contract.mapConfig(cfg))
		})
	}
}

func TestDeliveryOnlyOpenSearchIgnoresInvalidInactiveAsyncClientConfig(t *testing.T) {
	for _, handlerOnly := range []bool{false, true} {
		t.Run(strconv.FormatBool(handlerOnly), func(t *testing.T) {
			lines := deliveryOpenSearchClientEnv(t)
			if handlerOnly {
				lines = append(lines, "HETEROLOG_DELIVERY_LOG_TYPE=stdout", "HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER=opensearch")
			}
			lines = append(lines,
				"HETEROLOG_ASYNC_OPENSEARCH_TIMEOUT_SECOND=invalid",
				"HETEROLOG_ASYNC_OPENSEARCH_SKIP_VERIFY_SSL=invalid",
				"HETEROLOG_ENABLE_ASYNC_OPENSEARCH_COMPRESS_REQUEST=invalid",
				"HETEROLOG_ASYNC_OPENSEARCH_AUTH_TYPE=unsupported",
			)
			cfg, err := loadConfigErrorFromTempEnv(t, lines)
			require.NoError(t, err)
			require.Equal(t, "https://delivery.example:9200", cfg.DeliveryOpenSearchClient.Endpoint)
			mapped, err := MapDeliveryLoggerConfig(cfg)
			require.NoError(t, err)
			require.NotNil(t, mapped.OpenSearchClient)
			require.Equal(t, "https://delivery.example:9200", mapped.OpenSearchClient.Endpoint)
		})
	}
}

func TestDeliveryOpenSearchRejectsItsOwnInvalidClientConfig(t *testing.T) {
	requireEnvironmentValue := func(kind error) func(*testing.T, string, error) {
		return func(t *testing.T, key string, err error) {
			t.Helper()
			requireEnvironmentKeyError(t, err, key, kind)
		}
	}
	requireClientConfig := func(kind error) func(*testing.T, string, error) {
		return func(t *testing.T, _ string, err error) {
			t.Helper()
			fieldErr := requireLoggerConfigFieldError(
				t,
				err,
				"delivery logger",
				pkglogger.LogOutputOpenSearch,
				pkglogger.ConfigFieldClient,
				pkglogger.ErrInvalidClientConfig,
			)
			require.ErrorIs(t, fieldErr, kind)
		}
	}
	for _, tc := range []struct {
		key, value string
		check      func(*testing.T, string, error)
	}{
		{"HETEROLOG_DELIVERY_OPENSEARCH_TIMEOUT_SECOND", "invalid", requireEnvironmentValue(env.ErrNotInteger)},
		{"HETEROLOG_DELIVERY_OPENSEARCH_SKIP_VERIFY_SSL", "invalid", requireEnvironmentValue(env.ErrNotBoolean)},
		{"HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_COMPRESS_REQUEST", "invalid", requireEnvironmentValue(env.ErrNotBoolean)},
		{"HETEROLOG_DELIVERY_OPENSEARCH_TIMEOUT_SECOND", "-1", func(t *testing.T, _ string, err error) {
			t.Helper()
			requireTimeoutBudgetConfigError(
				t,
				err,
				pkglogger.LoggerContractDelivery,
				pkglogger.LogOutputOpenSearch,
				ErrNonPositiveClientTimeout,
			)
		}},
		{"HETEROLOG_DELIVERY_OPENSEARCH_ENDPOINT", "ftp://delivery.example", requireClientConfig(opensearch.ErrInvalidEndpoint)},
		{"HETEROLOG_DELIVERY_OPENSEARCH_AUTH_TYPE", "unsupported", requireClientConfig(opensearch.ErrUnsupportedAuthType)},
		{"HETEROLOG_DELIVERY_OPENSEARCH_AUTH_TYPE", "apikey", requireClientConfig(opensearch.ErrAPIKeyIDRequired)},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			cfg, err := loadConfigErrorFromTempEnv(t, append(deliveryOpenSearchClientEnv(t), tc.key+"="+tc.value))
			if err == nil {
				_, err = MapDeliveryLoggerConfig(cfg)
			}
			tc.check(t, tc.key, err)
		})
	}
}

func deliveryOpenSearchClientEnv(t *testing.T) []string {
	t.Helper()
	root := t.TempDir()
	return []string{
		"HETEROLOG_APP_NAME=delivery-opensearch-client-test",
		"HETEROLOG_TARGET=delivery-opensearch-client-test",
		"HETEROLOG_SYSTEM_LOG_DIR=" + filepath.Join(root, "log"),
		"HETEROLOG_SYSTEM_LIB_DIR=" + filepath.Join(root, "lib"),
		"HETEROLOG_APP_STATE_DIR=" + filepath.Join(root, "state"),
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_DELIVERY_LOG_TYPE=opensearch",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_DELIVERY_OPENSEARCH_ENDPOINT=https://delivery.example:9200",
		"HETEROLOG_DELIVERY_OPENSEARCH_AUTH_TYPE=none",
	}
}
