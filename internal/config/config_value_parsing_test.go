package config

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/timeoutbudget"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	"github.com/go-jappe/heterolog/pkg/env"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestConfigValueParsersDoNotReadEnvironment(t *testing.T) {
	const key = "TEST_CONFIG_VALUE"
	for _, tc := range []struct {
		name, environment, value, want string
		parse                          func(string) (string, error)
	}{
		{"compression", "gzip", " ZsTd ", string(fsstore.CompressZstd), func(value string) (string, error) {
			got, err := loadLogCompressFormat(value, key, fsstore.CompressGzip)
			return string(got), err
		}},
		{"level", "INFO", " debug ", pkglogger.LogLevelDebug.String(), func(value string) (string, error) {
			got, err := loadLogLevel(value, key, pkglogger.LogLevelInfo)
			return got.String(), err
		}},
		{"timeout", "must_fit", " MAY_CUTOFF ", string(timeoutbudget.TimeoutBudgetMayCutoff), func(value string) (string, error) {
			got, err := loadTimeoutBudgetPolicy(value, key)
			return string(got), err
		}},
		{"retry", "retry", " NONE ", string(pkglogger.RetryClientModeNone), func(value string) (string, error) {
			got, err := loadRetryClientMode(value, key, pkglogger.RetryClientModeRetry)
			return string(got), err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(key, tc.environment)
			if got, err := tc.parse(tc.value); err != nil || got != tc.want {
				t.Fatalf("parse(%q) = %q, %v; want %q", tc.value, got, err, tc.want)
			}
			// key自体を値として渡しても、環境変数の値へ置き換えてはならない。
			_, err := tc.parse(key)
			var valueErr *InvalidEnvironmentValueError
			if !errors.As(err, &valueErr) || valueErr.Key != key || valueErr.Value != key {
				t.Fatalf("parse(key) error = %v, want invalid value with setting key", err)
			}
		})
	}

	t.Setenv(key, "hourly")
	if got := loadCloudWatchLogsStreamRoutingGranularity(" DaIlY "); got != streamrouting.Daily {
		t.Fatalf("routing = %q, want daily", got)
	}
	if got := loadCloudWatchLogsStreamRoutingGranularity(key); got != streamrouting.Granularity(strings.ToLower(key)) {
		t.Fatalf("routing(key) = %q, must normalize the input, not read environment", got)
	}

	t.Setenv(key, "s3")
	values := []string{"S3", "", "S3", key}
	want := []objectstorage.UploadTarget{"s3", "", "s3", objectstorage.UploadTarget(strings.ToLower(key))}
	if got := objectStorageUploadTargets(values); !reflect.DeepEqual(got, want) {
		t.Fatalf("targets = %#v, want %#v (including empty/duplicate/unknown entries)", got, want)
	}
	if values[0] != "S3" {
		t.Fatal("conversion modified the input slice")
	}
}

func TestConfigValueParsingPreservesEnvironmentDefaultsAndErrors(t *testing.T) {
	const key = "TEST_CONFIG_VALUE_DEFAULT"
	for _, value := range []string{"", "invalid-value"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(key, value)
			format, formatErr := loadLogCompressFormat(env.GetEnvString(key, string(fsstore.CompressGzip)), key, fsstore.CompressGzip)
			level, levelErr := loadLogLevel(env.GetEnvString(key, pkglogger.LogLevelInfo.String()), key, pkglogger.LogLevelInfo)
			policy, policyErr := loadTimeoutBudgetPolicy(env.GetEnvString(key, string(timeoutbudget.DefaultTimeoutBudgetPolicy)), key)
			if format != fsstore.CompressGzip || level != pkglogger.LogLevelInfo {
				t.Fatalf("fallback values changed: format=%v, level=%v", format, level)
			}
			for _, err := range []error{formatErr, levelErr, policyErr} {
				if value == "" {
					if err != nil {
						t.Fatalf("default rejected: %v", err)
					}
				} else if valueErr := (*InvalidEnvironmentValueError)(nil); !errors.As(err, &valueErr) ||
					valueErr.Key != key || valueErr.Value != value {
					t.Fatalf("invalid value error = %v, want key and value", err)
				}
			}
			if value == "" && policy != timeoutbudget.DefaultTimeoutBudgetPolicy {
				t.Fatalf("default timeout policy = %q", policy)
			}
		})
	}
}

func TestFailureStoreTypeConvertersDoNotReadEnvironment(t *testing.T) {
	const key = "TEST_FAILURE_STORE_TYPE_VALUE"
	t.Setenv(key, "spool")
	values := []string{"NONE", "", "Unknown", "NONE", key}
	defaults := []failurestore.DurableType{failurestore.DurableTypeSpool}
	wantDurable := []failurestore.DurableType{"none", "", "unknown", "none", failurestore.DurableType(strings.ToLower(key))}
	if got := loadDurableTypes(values, defaults); !reflect.DeepEqual(got, wantDurable) {
		t.Fatalf("durable types = %v, want %v", got, wantDurable)
	}
	t.Setenv(key, "stdout")
	wantSpill := []pkgdurable.SpillType{"none", "", "unknown", "none", pkgdurable.SpillType(strings.ToLower(key))}
	if got := loadSpillTypes(values, nil); !reflect.DeepEqual(got, wantSpill) {
		t.Fatalf("spill types = %v, want %v", got, wantSpill)
	}
	if values[0] != "NONE" {
		t.Fatal("conversion modified the input slice")
	}
}

func TestRetryClientModeBlankUsesSuppliedDefault(t *testing.T) {
	const key = "TEST_RETRY_MODE_DEFAULT"
	t.Setenv(key, "not-a-mode")
	for _, raw := range []string{"", " \t "} {
		got, err := loadRetryClientMode(raw, key, pkglogger.RetryClientModeCircuitBreaker)
		if err != nil || got != pkglogger.RetryClientModeCircuitBreaker {
			t.Fatalf("loadRetryClientMode(%q) = %q, %v", raw, got, err)
		}
	}
}

func TestOptionalInternalLogBoolParsesValueWithoutReadingEnvironment(t *testing.T) {
	const key = "TEST_OPTIONAL_INTERNAL_LOG_BOOL"
	t.Setenv(key, "true")
	for _, tc := range []struct {
		raw                      string
		wantNil, want, wantError bool
	}{
		{raw: "", wantNil: true},
		{raw: "false"}, {raw: "0"}, {raw: "no"}, {raw: "off"},
		{raw: "true", want: true}, {raw: "1", want: true},
		{raw: "yes", want: true}, {raw: "on", want: true},
		{raw: key, wantError: true},
		{raw: "TRUE", wantError: true},
		{raw: " ", wantError: true},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			got, err := loadOptionalInternalLogBool(tc.raw, key)
			if tc.wantError {
				var valueErr *InvalidEnvironmentValueError
				if got != nil || !errors.As(err, &valueErr) || valueErr.Key != key || valueErr.Value != tc.raw {
					t.Fatalf("got %v, %v; want nil and error identifying key", got, err)
				}
				return
			}
			if err != nil || (got == nil) != tc.wantNil || (got != nil && *got != tc.want) {
				t.Fatalf("got %v, %v; want nil=%t, value=%t", got, err, tc.wantNil, tc.want)
			}
		})
	}
}
