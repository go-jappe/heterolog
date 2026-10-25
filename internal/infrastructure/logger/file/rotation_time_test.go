package file

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestFileRotationUsesClockTimezoneIndependentOfDisplay(t *testing.T) {
	utcTime := time.Date(2042, time.May, 5, 15, 30, 0, 0, time.UTC)
	for _, at := range []time.Time{utcTime, utcTime.In(time.FixedZone("JST", 9*60*60))} {
		t.Run(at.Location().String(), func(t *testing.T) {
			for _, role := range []loggerType{DefaultLogger, ErrorLogger, DebugLogger} {
				t.Run(string(role), func(t *testing.T) {
					logger := newInlineFilenamePurgeLogger(t, role, at, func(cfg *pkglogger.FileLoggerConfig) {
						cfg.FsstoreConfig.Lifecycle.EnableFileCompress = false
					})
					if err := rotateFileLoggerForTest(context.Background(), logger); err != nil {
						t.Fatal(err)
					}
					if got := filepath.Base(logger.Fsstore.ActiveFilePath); !strings.Contains(got, "_"+at.Format("20060102")+"_") {
						t.Fatalf("active path = %q, want Clock date %s", got, at.Format("20060102"))
					}
				})
			}
		})
	}
}
