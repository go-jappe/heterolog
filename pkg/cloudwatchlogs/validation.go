package cloudwatchlogs

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const maxLogStreamNameLength = 512

var (
	// ErrNegativeTimeout reports that Config.Timeout is negative.
	ErrNegativeTimeout = errors.New("cloudwatch logs timeout must not be negative")
	// ErrLogGroupNameRequired reports that Config.LogGroupName is empty or
	// contains only whitespace.
	ErrLogGroupNameRequired = errors.New("cloudwatch logs log group name is required")
	// ErrRegionRequired reports that Config.Region is empty or contains only
	// whitespace.
	ErrRegionRequired = errors.New("cloudwatch logs region is required")
	// ErrInvalidEndpoint reports that the endpoint cannot be parsed as a URL or
	// does not meet the constraints on the scheme, host, user info, query, and
	// fragment.
	ErrInvalidEndpoint = errors.New("invalid cloudwatch logs endpoint")
	// ErrCredentialsRequired reports that neither CredentialsProvider nor
	// AccessKeyID is set.
	ErrCredentialsRequired = errors.New("cloudwatch logs credentials provider or access key id is required")
	// ErrSecretAccessKeyRequired reports that there is no SecretAccessKey for
	// the AccessKeyID.
	ErrSecretAccessKeyRequired = errors.New("cloudwatch logs secret access key is required")

	// ErrLogStreamNameRequired reports that the log stream name is empty.
	ErrLogStreamNameRequired = errors.New("cloudwatch logs log stream name is required")
	// ErrLogStreamNameTooLong reports that the log stream name exceeds the
	// CloudWatch Logs limit on the number of characters.
	ErrLogStreamNameTooLong = errors.New("cloudwatch logs log stream name is too long")
	// ErrLogStreamNameInvalidCharacter reports that the log stream name
	// contains ':' or '*', which CloudWatch Logs does not accept.
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

// ValidateConfig validates the settings needed to create a CloudWatch Logs
// client. It does not retrieve credentials from CredentialsProvider or
// communicate over HTTP. The kind of problem can be identified by calling
// errors.Is with ErrNegativeTimeout, ErrLogGroupNameRequired,
// ErrRegionRequired, ErrInvalidEndpoint, ErrCredentialsRequired, or
// ErrSecretAccessKeyRequired.
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

// ValidateLogStreamName checks whether the actual stream name specified in one
// CloudWatch Logs API request meets the AWS constraints on the number of
// characters and the characters allowed. It does not handle the stream routing
// specific to heterolog, such as date suffixes. The kind of violation can be
// identified by calling errors.Is with ErrLogStreamNameRequired,
// ErrLogStreamNameTooLong, or ErrLogStreamNameInvalidCharacter.
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
