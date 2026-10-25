package opensearch

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/go-jappe/heterolog/pkg/internal/endpointurl"
)

var (
	// ErrBasicAuthUsernameRequiredは、Basic認証のusernameが空または空白だけであることを表します。
	ErrBasicAuthUsernameRequired = errors.New("opensearch basic auth username is required")
	// ErrBasicAuthPasswordRequiredは、Basic認証のpasswordが空または空白だけであることを表します。
	ErrBasicAuthPasswordRequired = errors.New("opensearch basic auth password is required")

	// ErrNegativeTimeoutは、Config.Timeoutが負値であることを表します。
	ErrNegativeTimeout = errors.New("opensearch timeout must not be negative")
	// ErrIndexRequiredは、indexが空または空白だけであることを表します。
	ErrIndexRequired = errors.New("opensearch index is required")
	// ErrIndexHasSurroundingWhitespaceは、indexの前後に空白があることを表します。
	ErrIndexHasSurroundingWhitespace = errors.New("opensearch index must not have surrounding whitespace")
	// ErrEndpointRequiredは、endpointが空または空白だけであることを表します。
	ErrEndpointRequired = errors.New("opensearch endpoint is required")
	// ErrEndpointHasSurroundingWhitespaceは、endpointの前後に空白があることを表します。
	ErrEndpointHasSurroundingWhitespace = errors.New("opensearch endpoint must not have surrounding whitespace")
	// ErrInvalidEndpointは、endpointをURLとして解釈できないか、schemeがhttp・httpsでないか、
	// hostがないことを表します。
	ErrInvalidEndpoint = errors.New("invalid opensearch endpoint")

	// ErrUnsupportedAuthTypeは、AuthConfig.Typeが対応する認証方式ではないことを表します。
	ErrUnsupportedAuthType = errors.New("unsupported opensearch auth type")
	// ErrInvalidAuthConfigTypeは、AuthConfig.Configの型がAuthConfig.Typeに対応しないことを表します。
	ErrInvalidAuthConfigType = errors.New("opensearch auth config has invalid type")
	// ErrAuthConfigNotAllowedは、AuthTypeNoneなのにAuthConfig.Configが指定されたことを表します。
	ErrAuthConfigNotAllowed = errors.New("opensearch auth config must be nil when auth type is none")
	// ErrAPIKeyIDRequiredは、API key認証のIDが空または空白だけであることを表します。
	ErrAPIKeyIDRequired = errors.New("opensearch api key id is required")
	// ErrAPIKeySecretRequiredは、API key認証のSecretが空または空白だけであることを表します。
	ErrAPIKeySecretRequired = errors.New("opensearch api key secret is required")
	// ErrAWSIAMRegionRequiredは、AWS IAM認証のRegionが空または空白だけであることを表します。
	ErrAWSIAMRegionRequired = errors.New("opensearch aws iam region is required")
	// ErrAWSIAMAccessKeyIDRequiredは、AWS IAM認証のAccessKeyIDが空または空白だけであることを表します。
	ErrAWSIAMAccessKeyIDRequired = errors.New("opensearch aws iam access key id is required")
	// ErrAWSIAMSecretAccessKeyRequiredは、AWS IAM認証のSecretAccessKeyが空または空白だけで
	// あることを表します。
	ErrAWSIAMSecretAccessKeyRequired = errors.New("opensearch aws iam secret access key is required")
	// ErrSAMLTokenRequiredは、SAML認証のTokenが空または空白だけであることを表します。
	ErrSAMLTokenRequired = errors.New("opensearch saml token is required")
	// ErrOIDCTokenRequiredは、OIDC認証のTokenが空または空白だけであることを表します。
	ErrOIDCTokenRequired = errors.New("opensearch oidc token is required")
	// ErrCertificateAuthRequiresHTTPSは、client証明書認証のendpointがhttpsではないことを表します。
	ErrCertificateAuthRequiresHTTPS = errors.New("opensearch certificate auth requires an https endpoint")
	// ErrCertificateFileRequiredは、client証明書認証のCertFileが空または空白だけであることを表します。
	ErrCertificateFileRequired = errors.New("opensearch certificate auth cert file is required")
	// ErrCertificateKeyFileRequiredは、client証明書認証のKeyFileが空または空白だけであることを表します。
	ErrCertificateKeyFileRequired = errors.New("opensearch certificate auth key file is required")
)

// validationErrorは、従来のmessageを保ったまま、errors.Isで種類を判別できる検証errorです。
// causeがあれば、それもerrors.Isやerrors.Asで辿れます。
type validationError struct {
	kind    error
	cause   error
	message string
}

func (e *validationError) Error() string { return e.message }

func (e *validationError) Unwrap() []error {
	if e.cause == nil {
		return []error{e.kind}
	}
	return []error{e.kind, e.cause}
}

// invalidAuthConfigTypeErrorは、authTypeに対応しないAuthConfig.Configの型を、
// ErrInvalidAuthConfigTypeを持つerrorとして返します。
func invalidAuthConfigTypeError(name string, config any) error {
	return &validationError{
		kind:    ErrInvalidAuthConfigType,
		message: fmt.Sprintf("opensearch %s auth config has invalid type: %T", name, config),
	}
}

// ValidateConfig はOpenSearch clientの生成に必要な設定を検証します。
// 証明書ファイルの読み込みやHTTP通信は行いません。
// Timeout==0はNewがdefault timeoutを適用する未指定sentinelですが、負値は設定エラーです。
// 不正の種類は、ErrNegativeTimeout、endpointのErrEndpointRequiredなど、このpackageの
// sentinelとerrors.Isで判別できます。
func ValidateConfig(cfg Config) error {
	if cfg.Timeout < 0 {
		return &validationError{
			kind:    ErrNegativeTimeout,
			message: fmt.Sprintf("opensearch timeout must not be negative: %s", cfg.Timeout),
		}
	}
	endpoint, err := validateEndpoint(cfg.Endpoint)
	if err != nil {
		return err
	}
	return validateAuthConfig(cfg.AuthConfig, endpoint.Scheme)
}

// ValidateIndex はloggerやspool senderがDocumentWriteRequestを作る前に、
// OpenSearchの配送先indexが空でないことを検証します。不正の種類は
// ErrIndexRequired、ErrIndexHasSurroundingWhitespaceとerrors.Isで判別できます。
func ValidateIndex(index string) error {
	if strings.TrimSpace(index) == "" {
		return ErrIndexRequired
	}
	if index != strings.TrimSpace(index) {
		return ErrIndexHasSurroundingWhitespace
	}
	return nil
}

func validateEndpoint(endpoint string) (*url.URL, error) {
	if strings.TrimSpace(endpoint) == "" {
		return nil, ErrEndpointRequired
	}
	if endpoint != strings.TrimSpace(endpoint) {
		return nil, ErrEndpointHasSurroundingWhitespace
	}

	// 解析できないendpointのerrorにも、URLのuser info（パスワードなど）を含めません。
	parsed, err := endpointurl.Parse(endpoint)
	if err != nil {
		return nil, &validationError{
			kind: ErrInvalidEndpoint, cause: err, message: fmt.Sprintf("invalid opensearch endpoint: %v", err),
		}
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https":
	default:
		return nil, &validationError{kind: ErrInvalidEndpoint, message: "opensearch endpoint scheme must be http or https"}
	}
	if parsed.Host == "" {
		return nil, &validationError{kind: ErrInvalidEndpoint, message: "opensearch endpoint must include host"}
	}
	return parsed, nil
}

func validateAuthConfig(auth AuthConfig, endpointScheme string) error {
	switch auth.Type {
	case AuthTypeBasic:
		cfg, ok := auth.Config.(BasicAuthConfig)
		if !ok {
			return invalidAuthConfigTypeError("basic", auth.Config)
		}
		if strings.TrimSpace(cfg.Username) == "" {
			return ErrBasicAuthUsernameRequired
		}
		if strings.TrimSpace(cfg.Password) == "" {
			return ErrBasicAuthPasswordRequired
		}
	case AuthTypeAPIKey:
		cfg, ok := auth.Config.(APIKeyAuthConfig)
		if !ok {
			return invalidAuthConfigTypeError("api key", auth.Config)
		}
		if strings.TrimSpace(cfg.ID) == "" {
			return ErrAPIKeyIDRequired
		}
		if strings.TrimSpace(cfg.Secret) == "" {
			return ErrAPIKeySecretRequired
		}
	case AuthTypeAWSIAM:
		cfg, ok := auth.Config.(AWSIAMAuthConfig)
		if !ok {
			return invalidAuthConfigTypeError("aws iam", auth.Config)
		}
		if strings.TrimSpace(cfg.Region) == "" {
			return ErrAWSIAMRegionRequired
		}
		if strings.TrimSpace(cfg.AccessKeyID) == "" {
			return ErrAWSIAMAccessKeyIDRequired
		}
		if strings.TrimSpace(cfg.SecretAccessKey) == "" {
			return ErrAWSIAMSecretAccessKeyRequired
		}
	case AuthTypeSAML:
		cfg, ok := auth.Config.(SAMLAuthConfig)
		if !ok {
			return invalidAuthConfigTypeError("saml", auth.Config)
		}
		if strings.TrimSpace(cfg.Token) == "" {
			return ErrSAMLTokenRequired
		}
	case AuthTypeOIDC:
		cfg, ok := auth.Config.(OIDCAuthConfig)
		if !ok {
			return invalidAuthConfigTypeError("oidc", auth.Config)
		}
		if strings.TrimSpace(cfg.Token) == "" {
			return ErrOIDCTokenRequired
		}
	case AuthTypeCertificate:
		cfg, ok := auth.Config.(CertificateAuthConfig)
		if !ok {
			return invalidAuthConfigTypeError("certificate", auth.Config)
		}
		if !strings.EqualFold(endpointScheme, "https") {
			return ErrCertificateAuthRequiresHTTPS
		}
		if strings.TrimSpace(cfg.CertFile) == "" {
			return ErrCertificateFileRequired
		}
		if strings.TrimSpace(cfg.KeyFile) == "" {
			return ErrCertificateKeyFileRequired
		}
	case AuthTypeNone:
		if auth.Config != nil {
			return ErrAuthConfigNotAllowed
		}
	default:
		return &validationError{
			kind:    ErrUnsupportedAuthType,
			message: fmt.Sprintf("unsupported opensearch auth type: %s", auth.Type),
		}
	}
	return nil
}
