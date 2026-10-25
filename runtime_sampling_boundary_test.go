package heterolog_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/go-jappe/heterolog"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestPublicConstructorRejectsUnrepresentableRequestSamplingRates(t *testing.T) {
	for _, rate := range []uint64{1<<32 - 1, 1 << 32, 1<<32 + 1} {
		t.Run(fmt.Sprint(rate), func(t *testing.T) {
			if strconv.IntSize < 64 {
				t.Skip("this boundary cannot be represented by a 32-bit int")
			}
			cfg := heterolog.DefaultConfig()
			cfg.Stdout.SamplingConfig = requestRateBoundaryConfig(int(rate))
			logger, err := heterolog.New(context.Background(), cfg)
			if rate > 1<<32-1 {
				if err == nil {
					_ = logger.Close()
					t.Fatal("constructor accepted a rate that truncates in the request-ID selector")
				}
				if !hasConfigFieldError(err, pkglogger.LogOutputStdout, pkglogger.ConfigFieldSamplingRequestIDRate, pkglogger.ErrInvalidSamplingRate) {
					t.Fatalf("error does not identify the invalid setting: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("largest supported rate rejected: %v", err)
			}
			defer logger.Close()
			if err := logger.Info(context.Background(), "boundary request", heterolog.WithRequestID("boundary-request")); err != nil {
				t.Fatalf("accepted configuration cannot safely log: %v", err)
			}
		})
	}
}

func TestPublicConstructorAcceptsRequestSamplingRateWithinNativeInt(t *testing.T) {
	// Exercise a large valid rate on both 32-bit and 64-bit systems.
	rate := int(^uint32(0) >> 1)
	cfg := heterolog.DefaultConfig()
	cfg.Stdout.SamplingConfig = requestRateBoundaryConfig(rate)
	logger, err := heterolog.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer logger.Close()
	if err := logger.Info(context.Background(), "native-int request", heterolog.WithRequestID("native-int-request")); err != nil {
		t.Fatal(err)
	}
}

func requestRateBoundaryConfig(rate int) pkglogger.SamplingConfig {
	return pkglogger.SamplingConfig{
		IsSamplingEnabled: true, EnableLoggingByRequestID: true, LoggingByRequestIDRate: rate,
		SamplingDebugRate: 2, SamplingInfoRate: 2, SamplingWarnRate: 2, SamplingErrorRate: 2,
	}
}
