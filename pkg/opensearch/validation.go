package opensearch

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/go-jappe/heterolog/pkg/internal/endpointurl"
)

var (
	// ErrBasicAuthUsernameRequired reports that the username for Basic
	// authentication is empty or contains only whitespace.
	ErrBasicAuthUsernameRequired = errors.New("opensearch basic auth username is required")
	// ErrBasicAuthPasswordRequired reports that the password for Basic
	// authentication is empty or contains only whitespace.
	ErrBasicAuthPasswordRequired = errors.New("opensearch basic auth password is required")

	// ErrNegativeTimeout reports that Config.Timeout is negative.
	ErrNegativeTimeout = errors.New("opensearch timeout must not be negative")
	// ErrIndexRequired reports that the index is empty or contains only whitespace.
	ErrIndexRequired = errors.New("opensearch index is required")
	// ErrIndexHasSurroundingWhitespace reports that the index has leading or
	// trailing whitespace.
	ErrIndexHasSurroundingWhitespace = errors.New("opensearch index must not have surrounding whitespace")
	// ErrEndpointRequired reports that the endpoint is empty or contains only
	// whitespace.
	ErrEndpointRequired = errors.New("opensearch endpoint is required")
	// ErrEndpointHasSurroundingWhitespace reports that the endpoint has leading
	// or trailing whitespace.
	ErrEndpointHasSurroundingWhitespace = errors.New("opensearch endpoint must not have surrounding whitespace")
	// ErrInvalidEndpoint reports that the endpoint cannot be parsed as a URL,
	// its scheme is not http or https, or it has no host.
	ErrInvalidEndpoint = errors.New("invalid opensearch endpoint")

	// ErrUnsupportedAuthType reports that AuthConfig.Type is not a supported
	// authentication method.
	ErrUnsupportedAuthType = errors.New("unsupported opensearch auth type")
	// ErrInvalidAuthConfigType reports that the type of AuthConfig.Config does
	// not correspond to AuthConfig.Type.
	ErrInvalidAuthConfigType = errors.New("opensearch auth config has invalid type")
	// ErrAuthConfigNotAllowed reports that AuthConfig.Config is set although
	// AuthConfig.Type is AuthTypeNone.
	ErrAuthConfigNotAllowed = errors.New("opensearch auth config must be nil when auth type is none")
	// ErrAPIKeyIDRequired reports that the ID for API key authentication is
	// empty or contains only whitespace.
	ErrAPIKeyIDRequired = errors.New("opensearch api key id is required")
	// ErrAPIKeySecretRequired reports that the Secret for API key authentication
	// is empty or contains only whitespace.
	ErrAPIKeySecretRequired = errors.New("opensearch api key secret is required")
	// ErrAWSIAMRegionRequired reports that the Region for AWS IAM authentication
	// is empty or contains only whitespace.
	ErrAWSIAMRegionRequired = errors.New("opensearch aws iam region is required")
	// ErrAWSIAMAccessKeyIDRequired reports that the AccessKeyID for AWS IAM
	// authentication is empty or contains only whitespace.
	ErrAWSIAMAccessKeyIDRequired = errors.New("opensearch aws iam access key id is required")
	// ErrAWSIAMSecretAccessKeyRequired reports that the SecretAccessKey for AWS
	// IAM authentication is empty or contains only whitespace.
	ErrAWSIAMSecretAccessKeyRequired = errors.New("opensearch aws iam secret access key is required")
	// ErrSAMLTokenRequired reports that the Token for SAML authentication is
	// empty or contains only whitespace.
	ErrSAMLTokenRequired = errors.New("opensearch saml token is required")
	// ErrOIDCTokenRequired reports that the Token for OIDC authentication is
	// empty or contains only whitespace.
	ErrOIDCTokenRequired = errors.New("opensearch oidc token is required")
	// ErrCertificateAuthRequiresHTTPS reports that the endpoint for client
	// certificate authentication is not https.
	ErrCertificateAuthRequiresHTTPS = errors.New("opensearch certificate auth requires an https endpoint")
	// ErrCertificateFileRequired reports that the CertFile for client
	// certificate authentication is empty or contains only whitespace.
	ErrCertificateFileRequired = errors.New("opensearch certificate auth cert file is required")
	// ErrCertificateKeyFileRequired reports that the KeyFile for client
	// certificate authentication is empty or contains only whitespace.
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

// ValidateConfig validates the settings needed to create an OpenSearch client.
// It does not read certificate files or communicate over HTTP. Timeout==0 is
// the unset sentinel for which New applies the default timeout, but a negative
// value is a configuration error. The kind of problem can be identified by
// calling errors.Is with the sentinels of this package, such as
// ErrNegativeTimeout and ErrEndpointRequired for the endpoint.
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

// ValidateIndex checks, before a logger or spool sender creates a
// DocumentWriteRequest, that the OpenSearch destination index is not empty. The
// kind of problem can be identified by calling errors.Is with ErrIndexRequired
// or ErrIndexHasSurroundingWhitespace.
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
