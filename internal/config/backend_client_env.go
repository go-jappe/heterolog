package config

import (
	"errors"

	clickhousecfg "github.com/go-jappe/heterolog/internal/config/clickhouse"
	"github.com/go-jappe/heterolog/internal/config/opensearch"
	pkgenv "github.com/go-jappe/heterolog/pkg/env"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

// openSearchClientKeysは、OpenSearchへの接続の環境変数のkeyです。ログの出力先（同期・
// 非同期・Delivery）とspanの送り先で、keyの名前の位置（HETEROLOG_ASYNC_OPENSEARCH_*など）
// だけが異なります。
type openSearchClientKeys struct {
	Endpoint, Timeout, SkipVerifySSL, CompressRequest              string
	AuthType, Username, Password, APIKeyID, APIKeySecret           string
	AWSRegion, AWSAccessKeyID, AWSSecretAccessKey, AWSSessionToken string
	SAMLToken, OIDCToken, CertFile, KeyFile, CAFile                string
}

var openSearchClientEnvKeys = openSearchClientKeys{
	Endpoint:           "HETEROLOG_OPENSEARCH_ENDPOINT",
	Timeout:            "HETEROLOG_OPENSEARCH_TIMEOUT_SECOND",
	SkipVerifySSL:      "HETEROLOG_OPENSEARCH_SKIP_VERIFY_SSL",
	CompressRequest:    "HETEROLOG_ENABLE_OPENSEARCH_COMPRESS_REQUEST",
	AuthType:           "HETEROLOG_OPENSEARCH_AUTH_TYPE",
	Username:           "HETEROLOG_OPENSEARCH_USERNAME",
	Password:           "HETEROLOG_OPENSEARCH_PASSWORD",
	APIKeyID:           "HETEROLOG_OPENSEARCH_API_KEY_ID",
	APIKeySecret:       "HETEROLOG_OPENSEARCH_API_KEY_SECRET",
	AWSRegion:          "HETEROLOG_OPENSEARCH_AWS_REGION",
	AWSAccessKeyID:     "HETEROLOG_OPENSEARCH_AWS_ACCESS_KEY_ID",
	AWSSecretAccessKey: "HETEROLOG_OPENSEARCH_AWS_SECRET_ACCESS_KEY",
	AWSSessionToken:    "HETEROLOG_OPENSEARCH_AWS_SESSION_TOKEN",
	SAMLToken:          "HETEROLOG_OPENSEARCH_SAML_TOKEN",
	OIDCToken:          "HETEROLOG_OPENSEARCH_OIDC_TOKEN",
	CertFile:           "HETEROLOG_OPENSEARCH_CERT_FILE",
	KeyFile:            "HETEROLOG_OPENSEARCH_KEY_FILE",
	CAFile:             "HETEROLOG_OPENSEARCH_CA_FILE",
}

var asyncOpenSearchClientEnvKeys = openSearchClientKeys{
	Endpoint:           "HETEROLOG_ASYNC_OPENSEARCH_ENDPOINT",
	Timeout:            "HETEROLOG_ASYNC_OPENSEARCH_TIMEOUT_SECOND",
	SkipVerifySSL:      "HETEROLOG_ASYNC_OPENSEARCH_SKIP_VERIFY_SSL",
	CompressRequest:    "HETEROLOG_ENABLE_ASYNC_OPENSEARCH_COMPRESS_REQUEST",
	AuthType:           "HETEROLOG_ASYNC_OPENSEARCH_AUTH_TYPE",
	Username:           "HETEROLOG_ASYNC_OPENSEARCH_USERNAME",
	Password:           "HETEROLOG_ASYNC_OPENSEARCH_PASSWORD",
	APIKeyID:           "HETEROLOG_ASYNC_OPENSEARCH_API_KEY_ID",
	APIKeySecret:       "HETEROLOG_ASYNC_OPENSEARCH_API_KEY_SECRET",
	AWSRegion:          "HETEROLOG_ASYNC_OPENSEARCH_AWS_REGION",
	AWSAccessKeyID:     "HETEROLOG_ASYNC_OPENSEARCH_AWS_ACCESS_KEY_ID",
	AWSSecretAccessKey: "HETEROLOG_ASYNC_OPENSEARCH_AWS_SECRET_ACCESS_KEY",
	AWSSessionToken:    "HETEROLOG_ASYNC_OPENSEARCH_AWS_SESSION_TOKEN",
	SAMLToken:          "HETEROLOG_ASYNC_OPENSEARCH_SAML_TOKEN",
	OIDCToken:          "HETEROLOG_ASYNC_OPENSEARCH_OIDC_TOKEN",
	CertFile:           "HETEROLOG_ASYNC_OPENSEARCH_CERT_FILE",
	KeyFile:            "HETEROLOG_ASYNC_OPENSEARCH_KEY_FILE",
	CAFile:             "HETEROLOG_ASYNC_OPENSEARCH_CA_FILE",
}

var deliveryOpenSearchClientEnvKeys = openSearchClientKeys{
	Endpoint:           "HETEROLOG_DELIVERY_OPENSEARCH_ENDPOINT",
	Timeout:            "HETEROLOG_DELIVERY_OPENSEARCH_TIMEOUT_SECOND",
	SkipVerifySSL:      "HETEROLOG_DELIVERY_OPENSEARCH_SKIP_VERIFY_SSL",
	CompressRequest:    "HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_COMPRESS_REQUEST",
	AuthType:           "HETEROLOG_DELIVERY_OPENSEARCH_AUTH_TYPE",
	Username:           "HETEROLOG_DELIVERY_OPENSEARCH_USERNAME",
	Password:           "HETEROLOG_DELIVERY_OPENSEARCH_PASSWORD",
	APIKeyID:           "HETEROLOG_DELIVERY_OPENSEARCH_API_KEY_ID",
	APIKeySecret:       "HETEROLOG_DELIVERY_OPENSEARCH_API_KEY_SECRET",
	AWSRegion:          "HETEROLOG_DELIVERY_OPENSEARCH_AWS_REGION",
	AWSAccessKeyID:     "HETEROLOG_DELIVERY_OPENSEARCH_AWS_ACCESS_KEY_ID",
	AWSSecretAccessKey: "HETEROLOG_DELIVERY_OPENSEARCH_AWS_SECRET_ACCESS_KEY",
	AWSSessionToken:    "HETEROLOG_DELIVERY_OPENSEARCH_AWS_SESSION_TOKEN",
	SAMLToken:          "HETEROLOG_DELIVERY_OPENSEARCH_SAML_TOKEN",
	OIDCToken:          "HETEROLOG_DELIVERY_OPENSEARCH_OIDC_TOKEN",
	CertFile:           "HETEROLOG_DELIVERY_OPENSEARCH_CERT_FILE",
	KeyFile:            "HETEROLOG_DELIVERY_OPENSEARCH_KEY_FILE",
	CAFile:             "HETEROLOG_DELIVERY_OPENSEARCH_CA_FILE",
}

// defaultOpenSearchClientConfigは、ログの出力先のOpenSearchへの接続の既定値です。
// 認証は、方式だけを既定にし、認証情報は補完しません。
var defaultOpenSearchClientConfig = opensearch.OpenSearchClientConfig{
	Endpoint:        opensearch.DefaultOpenSearchEndpoint,
	Timeout:         opensearch.DefaultOpenSearchTimeout,
	SkipVerifySSL:   opensearch.DefaultOpenSearchSkipVerify,
	Auth:            opensearch.OpenSearchAuthConfig{Type: opensearch.DefaultAuthType},
	CompressRequest: opensearch.DefaultCompressRequest,
}

// loadOpenSearchClientEnvは、OpenSearchへの接続の設定を、keysのkeyから読みます。keyが
// なければ、defaultsの値を使います。認証情報は暗黙に補完しません。basic認証の利用者名・
// passwordが未設定の場合、OpenSearchを使う構成は、mapper後のvalidationで設定エラーに
// なります。
func loadOpenSearchClientEnv(env pkgenv.Lookup, keys openSearchClientKeys, defaults opensearch.OpenSearchClientConfig) (opensearch.OpenSearchClientConfig, error) {
	timeout, timeoutErr := env.GetEnvDuration(keys.Timeout, defaults.Timeout)
	skipVerifySSL, skipVerifySSLErr := env.GetEnvBool(keys.SkipVerifySSL, defaults.SkipVerifySSL)
	compressRequest, compressRequestErr := env.GetEnvBool(keys.CompressRequest, defaults.CompressRequest)
	return opensearch.OpenSearchClientConfig{
		Endpoint:        env.GetEnvString(keys.Endpoint, defaults.Endpoint),
		Timeout:         timeout,
		SkipVerifySSL:   skipVerifySSL,
		Auth:            loadOpenSearchAuthEnv(env, keys, defaults.Auth.Type),
		CompressRequest: compressRequest,
	}, errors.Join(timeoutErr, skipVerifySSLErr, compressRequestErr)
}

// loadOpenSearchAuthEnvは、認証方式と、その方式の認証情報だけを読みます。未知の方式は
// そのまま保持し、mapper後のvalidationで拒否します。
func loadOpenSearchAuthEnv(env pkgenv.Lookup, keys openSearchClientKeys, defaultType pkgopensearch.AuthType) opensearch.OpenSearchAuthConfig {
	authType := pkgopensearch.AuthType(env.GetEnvString(keys.AuthType, string(defaultType)))
	auth := opensearch.OpenSearchAuthConfig{Type: authType}
	switch authType {
	case pkgopensearch.AuthTypeBasic:
		auth.Basic = &opensearch.BasicAuthConfig{
			Username: env.GetEnvString(keys.Username, ""),
			Password: env.GetEnvString(keys.Password, ""),
		}
	case pkgopensearch.AuthTypeAPIKey:
		auth.APIKey = &opensearch.APIKeyAuthConfig{
			ID:     env.GetEnvString(keys.APIKeyID, ""),
			Secret: env.GetEnvString(keys.APIKeySecret, ""),
		}
	case pkgopensearch.AuthTypeAWSIAM:
		auth.AWSIAM = &opensearch.AWSIAMAuthConfig{
			Region:          env.GetEnvString(keys.AWSRegion, ""),
			AccessKeyID:     env.GetEnvString(keys.AWSAccessKeyID, ""),
			SecretAccessKey: env.GetEnvString(keys.AWSSecretAccessKey, ""),
			SessionToken:    env.GetEnvString(keys.AWSSessionToken, ""),
		}
	case pkgopensearch.AuthTypeSAML:
		auth.SAML = &opensearch.SAMLAuthConfig{Token: env.GetEnvString(keys.SAMLToken, "")}
	case pkgopensearch.AuthTypeOIDC:
		auth.OIDC = &opensearch.OIDCAuthConfig{Token: env.GetEnvString(keys.OIDCToken, "")}
	case pkgopensearch.AuthTypeCertificate:
		auth.Certificate = &opensearch.CertificateAuthConfig{
			CertFile: env.GetEnvString(keys.CertFile, ""),
			KeyFile:  env.GetEnvString(keys.KeyFile, ""),
			CAFile:   env.GetEnvString(keys.CAFile, ""),
		}
	}
	return auth
}

// clickHouseClientKeysは、ClickHouseへの接続の環境変数のkeyです。ログの出力先（同期・
// 非同期・Delivery）とspanの送り先で、keyの名前の位置だけが異なります。
type clickHouseClientKeys struct {
	Endpoint, Database, Table, Timeout, Username, Password, Headers, CompressRequest string
}

var clickHouseClientEnvKeys = clickHouseClientKeys{
	Endpoint:        "HETEROLOG_CLICKHOUSE_ENDPOINT",
	Database:        "HETEROLOG_CLICKHOUSE_DATABASE",
	Table:           "HETEROLOG_CLICKHOUSE_TABLE",
	Timeout:         "HETEROLOG_CLICKHOUSE_TIMEOUT_SECOND",
	Username:        "HETEROLOG_CLICKHOUSE_USERNAME",
	Password:        "HETEROLOG_CLICKHOUSE_PASSWORD",
	Headers:         "HETEROLOG_CLICKHOUSE_HEADERS",
	CompressRequest: "HETEROLOG_ENABLE_CLICKHOUSE_COMPRESS_REQUEST",
}

var asyncClickHouseClientEnvKeys = clickHouseClientKeys{
	Endpoint:        "HETEROLOG_ASYNC_CLICKHOUSE_ENDPOINT",
	Database:        "HETEROLOG_ASYNC_CLICKHOUSE_DATABASE",
	Table:           "HETEROLOG_ASYNC_CLICKHOUSE_TABLE",
	Timeout:         "HETEROLOG_ASYNC_CLICKHOUSE_TIMEOUT_SECOND",
	Username:        "HETEROLOG_ASYNC_CLICKHOUSE_USERNAME",
	Password:        "HETEROLOG_ASYNC_CLICKHOUSE_PASSWORD",
	Headers:         "HETEROLOG_ASYNC_CLICKHOUSE_HEADERS",
	CompressRequest: "HETEROLOG_ENABLE_ASYNC_CLICKHOUSE_COMPRESS_REQUEST",
}

var deliveryClickHouseClientEnvKeys = clickHouseClientKeys{
	Endpoint:        "HETEROLOG_DELIVERY_CLICKHOUSE_ENDPOINT",
	Database:        "HETEROLOG_DELIVERY_CLICKHOUSE_DATABASE",
	Table:           "HETEROLOG_DELIVERY_CLICKHOUSE_TABLE",
	Timeout:         "HETEROLOG_DELIVERY_CLICKHOUSE_TIMEOUT_SECOND",
	Username:        "HETEROLOG_DELIVERY_CLICKHOUSE_USERNAME",
	Password:        "HETEROLOG_DELIVERY_CLICKHOUSE_PASSWORD",
	Headers:         "HETEROLOG_DELIVERY_CLICKHOUSE_HEADERS",
	CompressRequest: "HETEROLOG_ENABLE_DELIVERY_CLICKHOUSE_COMPRESS_REQUEST",
}

// defaultClickHouseClientConfigは、ログの出力先のClickHouseへの接続の既定値です。
var defaultClickHouseClientConfig = clickhousecfg.ClientConfig{
	Endpoint:        clickhousecfg.DefaultEndpoint,
	Database:        clickhousecfg.DefaultDatabase,
	Table:           clickhousecfg.DefaultTable,
	Timeout:         clickhousecfg.DefaultTimeout,
	Username:        clickhousecfg.DefaultUsername,
	Password:        clickhousecfg.DefaultPassword,
	CompressRequest: clickhousecfg.DefaultCompressRequest,
}

// loadClickHouseClientEnvは、ClickHouseへの接続の設定を、keysのkeyから読みます。keyが
// なければ、defaultsの値を使います。
func loadClickHouseClientEnv(env pkgenv.Lookup, keys clickHouseClientKeys, defaults clickhousecfg.ClientConfig) (clickhousecfg.ClientConfig, error) {
	timeout, timeoutErr := env.GetEnvDuration(keys.Timeout, defaults.Timeout)
	headers, headersErr := loadHeaderEnv(env, keys.Headers, defaults.Headers)
	compressRequest, compressRequestErr := env.GetEnvBool(keys.CompressRequest, defaults.CompressRequest)
	return clickhousecfg.ClientConfig{
		Endpoint:        env.GetEnvString(keys.Endpoint, defaults.Endpoint),
		Database:        env.GetEnvString(keys.Database, defaults.Database),
		Table:           env.GetEnvString(keys.Table, defaults.Table),
		Timeout:         timeout,
		Username:        env.GetEnvString(keys.Username, defaults.Username),
		Password:        env.GetEnvString(keys.Password, defaults.Password),
		Headers:         headers,
		CompressRequest: compressRequest,
	}, errors.Join(timeoutErr, headersErr, compressRequestErr)
}
