package heterolog_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-jappe/heterolog"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestPublicDeliveryObserverPanicIsUnconfirmed(t *testing.T) {
	for _, phase := range []string{"before_request", "after_acknowledgement", "no_panic"} {
		t.Run(phase, func(t *testing.T) {
			panicCause := errors.New("observer callback failed")
			var accepted atomic.Int32
			client := &http.Client{Transport: deliveryPanicTransport(func(r *http.Request) (*http.Response, error) {
				if r.URL.Host != "delivery-panic.invalid" || r.Method != http.MethodPost {
					return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL)
				}
				defer r.Body.Close()
				body, err := io.ReadAll(r.Body)
				if err != nil {
					return nil, err
				}
				if !strings.Contains(string(body), "delivery before observer panic") {
					return nil, fmt.Errorf("missing log message: %s", body)
				}
				accepted.Add(1)
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(`{}`)),
					Request:    r,
				}, nil
			})}
			cfg := deliveryEvidenceConfig("cloudwatchlogs", "http://delivery-panic.invalid")
			cfg.CloudWatchLogsClient.HTTPClient = client
			cfg.Stdout = heterolog.DefaultDeliveryConfig().Stdout
			cfg.LogOutputDestinations = append(cfg.LogOutputDestinations, pkglogger.LogOutputStdout)

			var report heterolog.DeliveryReport
			var callErr error
			output := captureRuntimeStdout(t, func() {
				logger, err := heterolog.NewDelivery(context.Background(), cfg,
					heterolog.WithObservers(deliveryPanicTracer{phase: phase, cause: panicCause}, nil))
				if err != nil {
					t.Fatal(err)
				}
				defer logger.Close()
				report, callErr = logger.Info(context.Background(), "delivery before observer panic")
				if err := logger.Close(); err != nil {
					t.Fatal(err)
				}
			})
			wantRequests := int32(1)
			if phase == "before_request" {
				wantRequests = 0
			}
			if accepted.Load() != wantRequests {
				t.Fatalf("accepted requests=%d, want %d for panic phase %s", accepted.Load(), wantRequests, phase)
			}
			if !strings.Contains(output, "delivery before observer panic") {
				t.Fatalf("sibling stdout delivery missing: %q", output)
			}
			if len(report.Targets) != 2 {
				t.Fatalf("targets=%+v, error=%v", report.Targets, callErr)
			}
			// The result recovered from the panic names its destination with the same
			// logical output name as a completed result.
			var cloudWatch pkglogger.DeliveryResult
			stdoutTargets, cloudWatchTargets := 0, 0
			for _, target := range report.Targets {
				switch target.Backend {
				case pkglogger.LogOutputStdout.String():
					stdoutTargets++
					if target.Status != pkglogger.DeliverySucceeded || target.Err != nil {
						t.Fatalf("observer panic changed sibling result: %+v", target)
					}
				case pkglogger.LogOutputCloudWatchLogs.String():
					cloudWatchTargets++
					cloudWatch = target
				default:
					t.Fatalf("target backend %q is not a logical output name: %+v", target.Backend, report)
				}
			}
			if stdoutTargets != 1 || cloudWatchTargets != 1 {
				t.Fatalf("stdout targets=%d cloudwatchlogs targets=%d, want exactly one each: %+v", stdoutTargets, cloudWatchTargets, report)
			}
			if phase == "no_panic" {
				if callErr != nil || cloudWatch.Status != pkglogger.DeliverySucceeded || report.HasUnregisteredTarget() {
					t.Fatalf("successful control report=%+v error=%v", report, callErr)
				}
				return
			}
			if !errors.Is(callErr, panicCause) || !errors.Is(cloudWatch.Err, panicCause) {
				t.Fatalf("panic cause was lost: target=%+v error=%v", cloudWatch, callErr)
			}
			if cloudWatch.Status != pkglogger.DeliveryUnconfirmed {
				t.Fatalf("panicking handler status=%s, want unconfirmed", cloudWatch.Status)
			}
			if report.HasTargetFailure() || len(report.FailedTargets()) != 0 || !report.HasUnregisteredTarget() {
				t.Fatalf("panic was treated as a confirmed backend failure or complete registration: %+v", report)
			}
		})
	}
}

type deliveryPanicTransport func(*http.Request) (*http.Response, error)

func (f deliveryPanicTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type deliveryPanicTracer struct {
	phase string
	cause error
}

func (tracer deliveryPanicTracer) StartSpan(ctx context.Context, name string, _ ...heterolog.ObserverAttr) (context.Context, heterolog.Span) {
	if name != "internal.infrastructure.logger.cloudwatchlogs.put" {
		return ctx, deliveryPanicSpan{}
	}
	if tracer.phase == "before_request" {
		panic(tracer.cause)
	}
	if tracer.phase == "after_acknowledgement" {
		return ctx, deliveryPanicSpan{cause: tracer.cause}
	}
	return ctx, deliveryPanicSpan{}
}

type deliveryPanicSpan struct{ cause error }

func (span deliveryPanicSpan) End() {
	if span.cause != nil {
		panic(span.cause)
	}
}
func (deliveryPanicSpan) TraceID() heterolog.TraceID                   { return "" }
func (deliveryPanicSpan) AddEvent(string, ...heterolog.ObserverAttr)   {}
func (deliveryPanicSpan) SetAttributes(...heterolog.ObserverAttr)      {}
func (deliveryPanicSpan) RecordError(error, ...heterolog.ObserverAttr) {}
