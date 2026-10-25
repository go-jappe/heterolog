package sampling

import (
	"errors"
	"testing"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestValidateSamplingConfigProviderRejectsNilConfig(t *testing.T) {
	if err := ValidateSamplingConfigProvider(nil); !errors.Is(err, ErrSamplingConfigRequired) {
		t.Fatalf("ValidateSamplingConfigProvider(nil) error = %v, want %v", err, ErrSamplingConfigRequired)
	}
}

// 規則はpkg/loggerのValidateSamplingConfigに委譲するため、不正な割合は
// ErrInvalidSamplingRateを持つConfigFieldErrorとして、項目ごとに特定できます。
func TestValidateSamplingConfigProviderRejectsEachNonPositiveRate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*pkglogger.SamplingConfig)
		field  pkglogger.ConfigField
	}{
		{name: "debug", mutate: func(cfg *pkglogger.SamplingConfig) { cfg.SamplingDebugRate = 0 }, field: pkglogger.ConfigFieldSamplingDebugRate},
		{name: "info", mutate: func(cfg *pkglogger.SamplingConfig) { cfg.SamplingInfoRate = 0 }, field: pkglogger.ConfigFieldSamplingInfoRate},
		{name: "warn", mutate: func(cfg *pkglogger.SamplingConfig) { cfg.SamplingWarnRate = 0 }, field: pkglogger.ConfigFieldSamplingWarnRate},
		{name: "error", mutate: func(cfg *pkglogger.SamplingConfig) { cfg.SamplingErrorRate = 0 }, field: pkglogger.ConfigFieldSamplingErrorRate},
		{
			name: "request_id",
			mutate: func(cfg *pkglogger.SamplingConfig) {
				cfg.EnableLoggingByRequestID = true
				cfg.LoggingByRequestIDRate = 0
			},
			field: pkglogger.ConfigFieldSamplingRequestIDRate,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := pkglogger.SamplingConfig{
				IsSamplingEnabled:      true,
				SamplingDebugRate:      1,
				SamplingInfoRate:       1,
				SamplingWarnRate:       1,
				SamplingErrorRate:      1,
				LoggingByRequestIDRate: 1,
			}
			tt.mutate(&cfg)

			for _, backend := range []pkglogger.LogOutputDestination{"", pkglogger.LogOutputStdout} {
				err := ValidateSamplingConfigProviderFor(backend, NewSamplingConfigAdapter(cfg))
				if !errors.Is(err, pkglogger.ErrInvalidSamplingRate) {
					t.Fatalf("backend %q: error = %v, want %v", backend, err, pkglogger.ErrInvalidSamplingRate)
				}
				fieldErrs := pkglogger.ConfigFieldErrors(err)
				if len(fieldErrs) != 1 || fieldErrs[0].Field != tt.field || fieldErrs[0].Backend != backend {
					t.Fatalf("backend %q: config field errors = %+v, want one %s error", backend, fieldErrs, tt.field)
				}
			}
		})
	}
}

func TestValidateSamplingConfigProviderAllowsUnusedZeroRates(t *testing.T) {
	tests := []struct {
		name string
		cfg  pkglogger.SamplingConfig
	}{
		{name: "sampling disabled", cfg: pkglogger.SamplingConfig{}},
		{
			name: "request id sampling disabled",
			cfg: pkglogger.SamplingConfig{
				IsSamplingEnabled: true,
				SamplingDebugRate: 1,
				SamplingInfoRate:  1,
				SamplingWarnRate:  1,
				SamplingErrorRate: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateSamplingConfigProvider(NewSamplingConfigAdapter(tt.cfg)); err != nil {
				t.Fatalf("ValidateSamplingConfigProvider() error = %v, want nil", err)
			}
		})
	}
}

// providerの検証はpkg/loggerの検証と同じ値を受理・拒否します。規則を二重に持たないため、
// 範囲の境界も含めて結果が一致することを確認します。
func TestValidateSamplingConfigProviderMatchesPackageRule(t *testing.T) {
	for _, rate := range []int{-1, 0, 1, 2} {
		for _, requestTracking := range []bool{false, true} {
			cfg := pkglogger.SamplingConfig{
				IsSamplingEnabled: true, EnableLoggingByRequestID: requestTracking, LoggingByRequestIDRate: rate,
				SamplingDebugRate: rate, SamplingInfoRate: 1, SamplingWarnRate: 1, SamplingErrorRate: 1,
			}
			want := pkglogger.ValidateSamplingConfig("", pkglogger.LogOutputStdout, cfg)
			got := ValidateSamplingConfigProviderFor(pkglogger.LogOutputStdout, NewSamplingConfigAdapter(cfg))
			if (got == nil) != (want == nil) || len(pkglogger.ConfigFieldErrors(got)) != len(pkglogger.ConfigFieldErrors(want)) {
				t.Fatalf("rate=%d request=%t: provider error = %v, package error = %v", rate, requestTracking, got, want)
			}
		}
	}
}
