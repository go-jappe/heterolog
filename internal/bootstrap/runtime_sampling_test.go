package bootstrap_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestRuntimeAppliesRequestSamplingAndMandatoryOutput(t *testing.T) {
	for _, mode := range []string{"request sampling", "mandatory info", "sampling disabled"} {
		t.Run(mode, func(t *testing.T) {
			cfg := bootstrap.DefaultConfig()
			cfg.Stdout.SamplingConfig = pkglogger.SamplingConfig{
				IsSamplingEnabled: true, EnableLoggingByRequestID: true, LoggingByRequestIDRate: 3,
				SamplingDebugRate: 5, SamplingInfoRate: 7, SamplingWarnRate: 11, SamplingErrorRate: 13,
			}
			if mode == "mandatory info" {
				cfg.Stdout.SamplingConfig.SamplingInfoRate = 1
			}
			if mode == "sampling disabled" {
				cfg.Stdout.SamplingConfig.IsSamplingEnabled = false
			}
			const requests = 64
			output := testutil.CaptureStdout(t, func() {
				logger, err := bootstrap.New(context.Background(), cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer logger.Close()
				for index := range requests {
					id := fmt.Sprintf("request-%02d", index)
					message := fmt.Sprintf("sampling-record-%02d", index)
					if err := logger.Info(context.Background(), message, pkglogger.WithRequestID(id)); err != nil {
						t.Fatal(err)
					}
				}
			})
			selected, skipped := 0, 0
			for index := range requests {
				id := fmt.Sprintf("request-%02d", index)
				keep := sampling.ShouldSampleByRequestId(id, cfg.Stdout.SamplingConfig.LoggingByRequestIDRate)
				if keep {
					selected++
				} else {
					skipped++
				}
				wantCount := 1
				if mode == "request sampling" && !keep {
					wantCount = 0
				}
				message := fmt.Sprintf("sampling-record-%02d", index)
				if got := strings.Count(output, message); got != wantCount {
					t.Fatalf("%s appears %d times, want %d", message, got, wantCount)
				}
			}
			if selected == 0 || skipped == 0 {
				t.Fatal("fixture must exercise both selected and excluded requests")
			}
			wantSampled := mode == "request sampling"
			for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
				if got := strings.Contains(line, " - sampled"); got != wantSampled {
					t.Fatalf("Sampled marker = %v, want %v: %s", got, wantSampled, line)
				}
			}
		})
	}
}
