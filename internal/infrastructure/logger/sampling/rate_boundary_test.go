package sampling_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
)

func TestRequestSamplingRejectsOutOfRangeDirectCallsWithoutPanicking(t *testing.T) {
	rates := []int{0, -1}
	if strconv.IntSize == 64 {
		overflow := uint64(1) << 32
		rates = append(rates, int(overflow), int(overflow+1))
	}
	for _, rate := range rates {
		t.Run(fmt.Sprint(rate), func(t *testing.T) {
			if sampling.ShouldSampleByRequestId("request", rate) {
				t.Fatal("invalid rate must not select a request")
			}
		})
	}
}

func TestRequestSamplingUpperLimitDoesNotRejectUnusedRates(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("out-of-range request rate cannot be represented by a 32-bit int")
	}
	large := uint64(1)<<32 + 1
	for _, mode := range []string{"sampling disabled", "request tracking disabled", "large level rate"} {
		t.Run(mode, func(t *testing.T) {
			cfg := requestSamplingConfig()
			switch mode {
			case "sampling disabled":
				cfg.IsSamplingEnabled = false
				cfg.LoggingByRequestIDRate = int(large)
			case "request tracking disabled":
				cfg.EnableLoggingByRequestID = false
				cfg.LoggingByRequestIDRate = int(large)
			case "large level rate":
				cfg.SamplingInfoRate = int(large)
			}
			if err := sampling.ValidateSamplingConfigProvider(sampling.NewSamplingConfigAdapter(cfg)); err != nil {
				t.Fatalf("unrelated or unused rate rejected: %v", err)
			}
		})
	}
}
