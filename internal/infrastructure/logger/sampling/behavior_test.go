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

// Discover examples instead of coupling the contract to particular hash values.
// The request selector is deterministic; no random draws or distribution bounds
// are involved in finding a request from each side of the selection.
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
			id = "" // Missing IDs must not activate the mandatory request-rate path.
		}
		for _, level := range samplingLevels {
			entry := &logging.LogEntry{Level: level, RequestID: id}
			process, sampled := sampling.ProcessLogSampling(entry, provider)
			// Either random outcome is valid. Only selected entries are marked
			// Sampled; neither path is mandatory when every level rate exceeds 1.
			if process != sampled {
				t.Fatalf("request tracking=%v at %s: process=%v sampled=%v", requestTracking, level, process, sampled)
			}
		}
	}
}

// Level sampling draws a random number for every entry, while request selection
// hashes the request ID: one ID is either always kept or always dropped. Repeating
// the same entry therefore separates the paths. Each other level gets a rate that
// keeps almost nothing, so reading another level's rate also leaves the range.
// The kept count follows Binomial(trials, 1/rate): mean 1000 and a standard
// deviation of about 27, so the accepted range is more than seven deviations wide
// on each side and a correct implementation practically never falls outside it.
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
		// The request rate of 1 would keep every entry if a missing ID selected the
		// request path.
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
