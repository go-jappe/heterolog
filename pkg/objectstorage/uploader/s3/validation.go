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
	// ErrRegionRequired reports that the region is empty after leading and
	// trailing white space is removed.
	ErrRegionRequired = errors.New("s3 region is required")
	// ErrBucketRequired reports that the bucket is empty after leading and
	// trailing white space is removed.
	ErrBucketRequired = errors.New("s3 bucket is required")
	// ErrNegativeTimeout reports that Timeout is negative.
	ErrNegativeTimeout = errors.New("s3 timeout must not be negative")
	// ErrBucketContainsPathSeparator reports that the bucket contains '/' or
	// '\'.
	ErrBucketContainsPathSeparator = errors.New("s3 bucket must not contain path separators")
	// ErrAccessKeyIDRequired reports that AccessKeyID is empty or only white
	// space.
	ErrAccessKeyIDRequired = errors.New("s3 access key id is required")
	// ErrSecretAccessKeyRequired reports that SecretAccessKey is empty or only
	// white space.
	ErrSecretAccessKeyRequired = errors.New("s3 secret access key is required")
	// ErrInvalidEndpoint reports that the endpoint cannot be parsed as a URL or
	// does not meet the constraints on its scheme, host, user info, query or
	// fragment.
	ErrInvalidEndpoint = errors.New("invalid s3 endpoint")
	// ErrNotRegularFile reports that the path of the file to upload is itself
	// not a regular file but a link, a directory or a special file.
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

// ValidateConfig validates the settings that the S3 client requires.
// Timeout==0 is the unset sentinel for which New applies the default timeout,
// but a negative value is a configuration error. The region and the bucket are
// checked for presence after leading and trailing white space is removed, and
// a missing one is identified by ErrRegionRequired or ErrBucketRequired
// respectively. A query or a fragment in the endpoint is not accepted. Other
// problems can also be identified with errors.Is against ErrNegativeTimeout,
// ErrBucketContainsPathSeparator, ErrAccessKeyIDRequired,
// ErrSecretAccessKeyRequired and ErrInvalidEndpoint.
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
	// proxyのpath segment内にあるencodeされたslashは保ったまま、endpointの区切りを
	// 除きます。Pathだけを更新するとRawPathが無効になり、失われます。
	parsed.RawPath = strings.TrimRight(parsed.EscapedPath(), "/")
	parsed.Path, err = url.PathUnescape(parsed.RawPath)
	if err != nil {
		return "", newValidationError(ErrInvalidEndpoint, err, "invalid s3 endpoint path: %v", err)
	}
	return parsed.String(), nil
}
