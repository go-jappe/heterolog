package compressedlog

import (
	"errors"
	"testing"

	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestRootRuntimeRejectsMixedMaintenanceModes(t *testing.T) {
	for _, firstDisabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal_first", true: "manual_first"}[firstDisabled], func(t *testing.T) {
			runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(managedUploaderTestConfig(t.TempDir())), objectstorage.UploadTargetS3))
			cfg := ManagedRouteConfig{DisableMaintenance: firstDisabled, FileName: replaytest.FileNamePattern("app_")}
			route, err := runtime.RegisterRoute(cfg, &replaytest.Uploader{})
			if err != nil {
				t.Fatal(err)
			}
			defer route.Close()
			cfg.DisableMaintenance = !firstDisabled
			if _, err := runtime.RegisterRoute(cfg, &replaytest.Uploader{}); !errors.Is(err, ErrMaintenanceModeMismatch) {
				t.Fatalf("mixed-mode registration = %v, want mode mismatch", err)
			}
			if firstDisabled {
				select {
				case <-runtime.runLoopStopped:
				default:
					t.Fatal("manual route started a maintenance loop")
				}
			}
		})
	}
}
