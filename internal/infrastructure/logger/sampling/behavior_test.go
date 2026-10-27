package sampling_test

import (
	"fmt"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

var samplingLevels = []pkglogger.LogLevel{
	pkglogger.LogLevelDebug, pkglogger.LogLevelInfo, pkglogger.LogLevelWarn, pkglogger.LogLevelError,
}

func requestSamplingConfig() pkglogger.SamplingConfig {
	return pkglogger.SamplingConfig{
		IsSamplingEnabled: true, EnableLoggingByRequestID: true, LoggingByRequestIDRate: 3,
		SamplingDebugRate: 5, SamplingInfoRate: 7, SamplingWarnRate: 11, SamplingErrorRate: 13,
	}
}

// 契約を特定のhash値へ結び付けず、例を探索して見つけます。
// request selectorは決定的なため、選択される側とされない側のrequestを見つけるのに、
// 乱数の抽選や分布に基づく範囲は関係しません。
func requestSamplingExamples(t *testing.T, rate int) (selected, skipped string) {
	t.Helper()
	for index := 0; index < 4096 && (selected == "" || skipped == ""); index++ {
		id := fmt.Sprintf("sampling-request-%d", index)
		if sampling.ShouldSampleByRequestId(id, rate) {
			selected = id
		} else {
			skipped = id
		}
	}
	if selected == "" || skipped == "" {
		t.Fatal("request sampling must both select and omit requests at a rate greater than one")
	}
	return selected, skipped
}

func TestRequestSamplingKeepsTheSameDecisionAcrossEntriesAndLevels(t *testing.T) {
	cfg := requestSamplingConfig()
	selected, skipped := requestSamplingExamples(t, cfg.LoggingByRequestIDRate)
	provider := sampling.NewSamplingConfigAdapter(cfg)
	for _, example := range []struct {
		id   string
		keep bool
	}{{selected, true}, {skipped, false}} {
		for _, level := range samplingLevels {
			for index := range 16 {
				entry := &logging.LogEntry{
					RequestID: example.id, Level: level,
					Message: fmt.Sprintf("entry %d", index), TraceID: fmt.Sprintf("trace-%d", index),
				}
				process, sampled := sampling.ProcessLogSampling(entry, provider)
				if process != example.keep || sampled != example.keep {
					t.Fatalf("request %q at %s: process=%v sampled=%v, want both %v", example.id, level, process, sampled, example.keep)
				}
			}
		}
	}
}

func TestSamplingMandatoryEntriesOverrideRequestSelection(t *testing.T) {
	base := requestSamplingConfig()
	_, skipped := requestSamplingExamples(t, base.LoggingByRequestIDRate)
	for _, level := range samplingLevels {
		t.Run(level.String(), func(t *testing.T) {
			cfg := base
			switch level {
			case pkglogger.LogLevelDebug:
				cfg.SamplingDebugRate = 1
			case pkglogger.LogLevelInfo:
				cfg.SamplingInfoRate = 1
			case pkglogger.LogLevelWarn:
				cfg.SamplingWarnRate = 1
			case pkglogger.LogLevelError:
				cfg.SamplingErrorRate = 1
			}
			entry := &logging.LogEntry{Level: level, RequestID: skipped, Sampled: true}
			provider := sampling.NewSamplingConfigAdapter(cfg)
			if process, sampled := sampling.ProcessLogSampling(entry, provider); !process || sampled {
				t.Fatalf("mandatory entry: process=%v sampled=%v, want true, false", process, sampled)
			}
			if !sampling.ApplySamplingLogic(entry, provider) || entry.Sampled {
				t.Fatal("mandatory entry must be emitted with Sampled=false")
			}
		})
	}
}

func TestSamplingDisabledAndFullRequestRateEmitWithoutSampledFlag(t *testing.T) {
	fullRequest := requestSamplingConfig()
	fullRequest.LoggingByRequestIDRate = 1
	for _, test := range []struct {
		name string
		cfg  pkglogger.SamplingConfig
	}{{"disabled with unused zero rates", pkglogger.SamplingConfig{}}, {"all requests selected", fullRequest}} {
		t.Run(test.name, func(t *testing.T) {
			provider := sampling.NewSamplingConfigAdapter(test.cfg)
			for _, level := range samplingLevels {
				entry := &logging.LogEntry{Level: level, RequestID: "whole-request", Sampled: true}
				if process, sampled := sampling.ProcessLogSampling(entry, provider); !process || sampled {
					t.Fatalf("%s: process=%v sampled=%v, want true, false", level, process, sampled)
				}
				if !sampling.ApplySamplingLogic(entry, provider) || entry.Sampled {
					t.Fatalf("%s must be emitted with Sampled=false", level)
				}
			}
		})
	}
}

func TestSamplingFallsBackToLevelWhenRequestTrackingIsUnavailable(t *testing.T) {
	for _, requestTracking := range []bool{false, true} {
		cfg := requestSamplingConfig()
		cfg.EnableLoggingByRequestID = requestTracking
		cfg.LoggingByRequestIDRate = 1
		provider := sampling.NewSamplingConfigAdapter(cfg)
		id := "ignored-request"
		if requestTracking {
			id = "" // IDがない場合に、request rateによる必須出力の経路を有効にしてはいけません。
		}
		for _, level := range samplingLevels {
			entry := &logging.LogEntry{Level: level, RequestID: id}
			process, sampled := sampling.ProcessLogSampling(entry, provider)
			// 乱数による結果はどちらでも有効です。選ばれたentryだけにSampledが付きます。
			// level rateがすべて1を超える場合、どちらの経路も必須出力にはなりません。
			if process != sampled {
				t.Fatalf("request tracking=%v at %s: process=%v sampled=%v", requestTracking, level, process, sampled)
			}
		}
	}
}

// level samplingはentryごとに乱数を引きますが、request selectionはrequest IDをhashするため、
// 一つのIDは常に残るか常に捨てられるかのどちらかです。そのため、同じentryを繰り返すと
// 二つの経路を区別できます。他のlevelにはほぼ何も残さないrateを与えるので、別のlevelの
// rateを読んだ場合も範囲から外れます。残る件数はBinomial(trials, 1/rate)に従い、
// 平均は1000、標準偏差は約27です。そのため許容範囲は片側で標準偏差の7倍を超える幅があり、
// 正しい実装が範囲外になることは実質的にありません。
func TestLevelSamplingKeepsAboutOneInRateEntriesWithoutRequestSelection(t *testing.T) {
	const (
		rate      = 4
		otherRate = 1_000_000
		trials    = 4000
		minKept   = 800
		maxKept   = 1200
	)
	for _, mode := range []struct {
		name            string
		requestTracking bool
		requestID       string
	}{
		{name: "request tracking disabled", requestID: "ignored-request"},
		// IDがないのにrequestの経路を選ぶと、request rateが1なのですべてのentryが残ってしまいます。
		{name: "request ID missing", requestTracking: true},
	} {
		for _, level := range samplingLevels {
			t.Run(mode.name+"/"+level.String(), func(t *testing.T) {
				cfg := pkglogger.SamplingConfig{
					IsSamplingEnabled: true, EnableLoggingByRequestID: mode.requestTracking, LoggingByRequestIDRate: 1,
					SamplingDebugRate: otherRate, SamplingInfoRate: otherRate, SamplingWarnRate: otherRate, SamplingErrorRate: otherRate,
				}
				switch level {
				case pkglogger.LogLevelDebug:
					cfg.SamplingDebugRate = rate
				case pkglogger.LogLevelInfo:
					cfg.SamplingInfoRate = rate
				case pkglogger.LogLevelWarn:
					cfg.SamplingWarnRate = rate
				case pkglogger.LogLevelError:
					cfg.SamplingErrorRate = rate
				}
				provider := sampling.NewSamplingConfigAdapter(cfg)
				kept := 0
				for range trials {
					entry := &logging.LogEntry{Level: level, RequestID: mode.requestID}
					process, sampled := sampling.ProcessLogSampling(entry, provider)
					if process != sampled {
						t.Fatalf("process=%v sampled=%v: a kept level-sampled entry must be marked Sampled", process, sampled)
					}
					if process {
						kept++
					}
				}
				if kept < minKept || kept > maxKept {
					t.Fatalf("kept %d of %d entries at rate %d, want between %d and %d", kept, trials, rate, minKept, maxKept)
				}
			})
		}
	}
}

func TestApplySamplingLogicSelectsRequestsAndMarksAcceptedEntries(t *testing.T) {
	cfg := requestSamplingConfig()
	selected, skipped := requestSamplingExamples(t, cfg.LoggingByRequestIDRate)
	provider := sampling.NewSamplingConfigAdapter(cfg)
	entry := &logging.LogEntry{Level: pkglogger.LogLevelInfo, RequestID: selected}
	if !sampling.ApplySamplingLogic(entry, provider) || !entry.Sampled {
		t.Fatal("selected request must be emitted with Sampled=true")
	}
	entry = &logging.LogEntry{Level: pkglogger.LogLevelInfo, RequestID: skipped}
	if sampling.ApplySamplingLogic(entry, provider) {
		t.Fatal("unselected request must not be emitted")
	}
}
