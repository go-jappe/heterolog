package compressedlog

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
)

// upload先が上限を超えるRetry-Afterを返した周期だけ、宛先・要求時間・上限を含む警告をOnWarnへ渡す。
func TestRootRuntimeWarnsWhenDestinationRetryAfterIsCapped(t *testing.T) {
	cfg := managedUploaderTestConfig(t.TempDir())
	cfg.RunnerInterval = time.Hour
	cfg.MaxRetryAfter = 30 * time.Minute
	runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
	var warnings []error
	upload := &replaytest.Uploader{DestinationID: "s3-throttled", Err: &pkgs3.HTTPStatusError{
		Code:               http.StatusServiceUnavailable,
		RetryAfterDuration: 3 * time.Hour,
	}}
	route, err := runtime.RegisterRoute(ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("app_"),
		OnWarn:   func(_ context.Context, warning error) { warnings = append(warnings, warning) },
	}, upload)
	if err != nil {
		t.Fatal(err)
	}
	defer route.Close()
	if _, err := route.SaveRetryableFailure(context.Background(), replaytest.WriteSource(t, "app_20261005_1.log.gz")); err != nil {
		t.Fatal(err)
	}

	report, err := runtime.runner.RunOnce(context.Background())
	if err == nil || len(report.CompressedLogFiles) != 1 || report.CompressedLogFiles[0].RetryAfterClamp == nil {
		t.Fatalf("throttled RunOnce() report/error = %+v/%v, want a capped Retry-After", report, err)
	}
	runtime.ReportSpoolRunnerRun(context.Background(), report, err)
	var clamped *RetryAfterClampWarning
	if len(warnings) != 1 || !errors.Is(warnings[0], ErrRetryAfterClamped) || !errors.As(warnings[0], &clamped) {
		t.Fatalf("warnings = %v, want one capped Retry-After warning", warnings)
	}
	want := RetryAfterClampWarning{
		DestinationID: "s3-throttled",
		Requested:     3 * time.Hour,
		MaxRetryAfter: 30 * time.Minute,
		NotBefore:     report.CompressedLogFiles[0].RetryAfterClamp.NotBefore,
	}
	if *clamped != want {
		t.Fatalf("capped Retry-After warning = %+v, want %+v", *clamped, want)
	}

	// 期限中の周期は短縮を伴わないため、同じ警告を繰り返さない。
	deferred, err := runtime.runner.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	runtime.ReportSpoolRunnerRun(context.Background(), deferred, nil)
	if len(warnings) != 1 {
		t.Fatalf("warnings after a deferred cycle = %v, want no repeated warning", warnings)
	}
}

func TestNewRootRuntimeRejectsNegativeMaxRetryAfter(t *testing.T) {
	cfg := managedUploaderTestConfig(t.TempDir())
	cfg.MaxRetryAfter = -time.Second
	runtime, err := NewRootRuntime(NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
	if runtime != nil {
		_ = runtime.Close()
	}
	if !errors.Is(err, ErrInvalidCompressedLogFileUploadSpoolConfig) {
		t.Fatalf("NewRootRuntime() error = %v, want invalid config", err)
	}
}
