package cloudwatchlogs

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const maxLogStreamNameLength = 512

var (
	// ErrNegativeTimeoutは、Config.Timeoutが負値であることを表します。
	ErrNegativeTimeout = errors.New("cloudwatch logs timeout must not be negative")
	// ErrLogGroupNameRequiredは、Config.LogGroupNameが空または空白だけであることを表します。
	ErrLogGroupNameRequired = errors.New("cloudwatch logs log group name is required")
	// ErrRegionRequiredは、Config.Regionが空または空白だけであることを表します。
	ErrRegionRequired = errors.New("cloudwatch logs region is required")
	// ErrInvalidEndpointは、endpointをURLとして解釈できないか、scheme、host、user info、
	// query、fragmentの制約を満たさないことを表します。
	ErrInvalidEndpoint = errors.New("invalid cloudwatch logs endpoint")
	// ErrCredentialsRequiredは、CredentialsProviderとAccessKeyIDのどちらも指定されて
	// いないことを表します。
	ErrCredentialsRequired = errors.New("cloudwatch logs credentials provider or access key id is required")
	// ErrSecretAccessKeyRequiredは、AccessKeyIDに対応するSecretAccessKeyがないことを表します。
	ErrSecretAccessKeyRequired = errors.New("cloudwatch logs secret access key is required")

	// ErrLogStreamNameRequiredは、log stream名が空であることを表します。
	ErrLogStreamNameRequired = errors.New("cloudwatch logs log stream name is required")
	// ErrLogStreamNameTooLongは、log stream名がCloudWatch Logsの文字数上限を超えることを表します。
	ErrLogStreamNameTooLong = errors.New("cloudwatch logs log stream name is too long")
	// ErrLogStreamNameInvalidCharacterは、log stream名にCloudWatch Logsが受け付けない
	// ':'または'*'が含まれることを表します。
	ErrLogStreamNameInvalidCharacter = errors.New("cloudwatch logs log stream name contains an invalid character")
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

// ValidateConfig はCloudWatch Logs clientの生成に必要な設定を検証します。
// CredentialsProviderからの認証情報取得やHTTP通信は行いません。不正の種類は
// ErrNegativeTimeout、ErrLogGroupNameRequired、ErrRegionRequired、ErrInvalidEndpoint、
// ErrCredentialsRequired、ErrSecretAccessKeyRequiredとerrors.Isで判別できます。
func ValidateConfig(cfg Config) error {
	if cfg.Timeout < 0 {
		return &validationError{
			kind:    ErrNegativeTimeout,
			message: fmt.Sprintf("cloudwatch logs timeout must not be negative: %s", cfg.Timeout),
		}
	}
	if strings.TrimSpace(cfg.LogGroupName) == "" {
		return ErrLogGroupNameRequired
	}
	region := strings.TrimSpace(cfg.Region)
	if region == "" {
		return ErrRegionRequired
	}
	if _, err := normalizeEndpoint(cfg.Endpoint, region); err != nil {
		return err
	}
	return validateCredentialsConfig(cfg)
}

// ValidateLogStreamNameは、一回のCloudWatch Logs API requestで指定する実stream名が
// AWSの文字数・使用文字制約を満たすかを検証します。日付suffixなど、本アプリ固有の
// stream routingは扱いません。違反の種類はErrLogStreamNameRequired、
// ErrLogStreamNameTooLong、ErrLogStreamNameInvalidCharacterとerrors.Isで判別できます。
func ValidateLogStreamName(name string) error {
	if name == "" {
		return ErrLogStreamNameRequired
	}
	characterCount := utf8.RuneCountInString(name)
	if characterCount > maxLogStreamNameLength {
		return &validationError{kind: ErrLogStreamNameTooLong, message: fmt.Sprintf(
			"cloudwatch logs log stream name length %d exceeds %d",
			characterCount,
			maxLogStreamNameLength,
		)}
	}
	if strings.ContainsAny(name, ":*") {
		return &validationError{kind: ErrLogStreamNameInvalidCharacter, message: fmt.Sprintf(
			"cloudwatch logs log stream name %q must not contain ':' or '*'",
			name,
		)}
	}
	return nil
}

func validateCredentialsConfig(cfg Config) error {
	if cfg.CredentialsProvider != nil {
		return nil
	}
	if strings.TrimSpace(cfg.AccessKeyID) == "" {
		return ErrCredentialsRequired
	}
	if strings.TrimSpace(cfg.SecretAccessKey) == "" {
		return ErrSecretAccessKeyRequired
	}
	return nil
}
