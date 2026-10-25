package s3

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/go-jappe/heterolog/pkg/internal/endpointurl"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

var (
	// ErrRegionRequiredは、前後の空白を除いたregionが空であることを表します。
	ErrRegionRequired = errors.New("s3 region is required")
	// ErrBucketRequiredは、前後の空白を除いたbucketが空であることを表します。
	ErrBucketRequired = errors.New("s3 bucket is required")
	// ErrNegativeTimeoutは、Timeoutが負値であることを表します。
	ErrNegativeTimeout = errors.New("s3 timeout must not be negative")
	// ErrBucketContainsPathSeparatorは、bucketに'/'または'\'が含まれることを表します。
	ErrBucketContainsPathSeparator = errors.New("s3 bucket must not contain path separators")
	// ErrAccessKeyIDRequiredは、AccessKeyIDが空または空白だけであることを表します。
	ErrAccessKeyIDRequired = errors.New("s3 access key id is required")
	// ErrSecretAccessKeyRequiredは、SecretAccessKeyが空または空白だけであることを表します。
	ErrSecretAccessKeyRequired = errors.New("s3 secret access key is required")
	// ErrInvalidEndpointは、endpointをURLとして解釈できないか、scheme、host、user info、
	// query、fragmentの制約を満たさないことを表します。
	ErrInvalidEndpoint = errors.New("invalid s3 endpoint")
	// ErrNotRegularFileは、送信するfileのpath自身がregular fileではなく、linkや
	// directory、特殊fileであることを表します。
	ErrNotRegularFile = errors.New("upload file path is not a regular file")
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

func newValidationError(kind, cause error, format string, args ...any) error {
	return &validationError{kind: kind, cause: cause, message: fmt.Sprintf(format, args...)}
}

// ValidateConfig は、S3 clientが必要とする設定値を検証します。
// Timeout==0はNewがdefault timeoutを適用する未指定sentinelですが、
// 負値は設定エラーです。regionとbucketは前後の空白を除いた値で必須性を検証し、
// 欠落はそれぞれErrRegionRequired、ErrBucketRequiredで判別できます。
// endpointのqueryとfragmentは受け付けません。そのほかの不正もErrNegativeTimeout、
// ErrBucketContainsPathSeparator、ErrAccessKeyIDRequired、ErrSecretAccessKeyRequired、
// ErrInvalidEndpointとerrors.Isで判別できます。
func ValidateConfig(cfg objectstorage.S3Config) error {
	if cfg.Timeout < 0 {
		return newValidationError(ErrNegativeTimeout, nil, "s3 timeout must not be negative: %s", cfg.Timeout)
	}
	region := strings.TrimSpace(cfg.Region)
	if region == "" {
		return ErrRegionRequired
	}
	bucket := strings.TrimSpace(cfg.Bucket)
	if bucket == "" {
		return ErrBucketRequired
	}
	if strings.ContainsAny(bucket, `/\`) {
		return ErrBucketContainsPathSeparator
	}
	if strings.TrimSpace(cfg.AccessKeyID) == "" {
		return ErrAccessKeyIDRequired
	}
	if strings.TrimSpace(cfg.SecretAccessKey) == "" {
		return ErrSecretAccessKeyRequired
	}
	if _, err := normalizeEndpoint(cfg.Endpoint, region); err != nil {
		return err
	}
	return nil
}

// normalizeRegionは、宛先IDのために前後の空白を除いたregionをlowercaseへ正規化します。
// 大文字小文字だけが異なるregionは同じ保存先として扱います。SigV4署名とdefault endpointには
// 正規化せず、入力値の前後の空白だけを除いたregionを使います。
func normalizeRegion(region string) string {
	return strings.ToLower(strings.TrimSpace(region))
}

// normalizeEndpoint は空のendpointをAWS S3 endpointで補完し、clientが保持する形式へ正規化します。
// ValidateConfigとNewはこの関数を共有し、endpointの受理条件と実際の利用形式を一致させます。
// regionには前後の空白を除いた値を渡します。
func normalizeEndpoint(endpoint string, region string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		endpoint = fmt.Sprintf("https://s3.%s.amazonaws.com", region)
	}
	// 解析できないendpointのerrorにも、URLのuser info（パスワードなど）を含めません。
	parsed, err := endpointurl.Parse(endpoint)
	if err != nil {
		return "", newValidationError(ErrInvalidEndpoint, err, "invalid s3 endpoint: %v", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", newValidationError(ErrInvalidEndpoint, nil, "s3 endpoint must include scheme and host")
	}
	if parsed.User != nil {
		return "", newValidationError(ErrInvalidEndpoint, nil, "s3 endpoint must not include user info")
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", newValidationError(ErrInvalidEndpoint, nil, "s3 endpoint scheme must be http or https")
	}
	// objectURLはendpointのqueryをすべてのPUTへ引き継いでしまうため、空queryを含めて
	// queryとfragmentは設定errorにします。
	if parsed.ForceQuery || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", newValidationError(ErrInvalidEndpoint, nil, "s3 endpoint must not include query or fragment")
	}
	// Remove endpoint delimiters while retaining any encoded slash inside a
	// proxy path segment. Updating only Path would invalidate RawPath and lose it.
	parsed.RawPath = strings.TrimRight(parsed.EscapedPath(), "/")
	parsed.Path, err = url.PathUnescape(parsed.RawPath)
	if err != nil {
		return "", newValidationError(ErrInvalidEndpoint, err, "invalid s3 endpoint path: %v", err)
	}
	return parsed.String(), nil
}
