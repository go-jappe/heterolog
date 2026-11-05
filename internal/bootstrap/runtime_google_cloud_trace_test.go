package bootstrap

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// Cloud Runのtraceparentを読んだcontextで書いたログが、組み立てたloggerからCloud Traceへ
// 関連付く形で標準出力へ出ることを確かめます。
func TestStdoutLinksLogsToCloudTraceFromTraceParent(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Stdout.UseGoogleCloudLoggingFormat = true
	cfg.Stdout.GoogleCloudProjectID = "my-project"

	sc, err := pkgobserver.ParseTraceParent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	if err != nil {
		t.Fatal(err)
	}
	ctx := pkgobserver.ContextWithSpanContext(context.Background(), sc)

	output := testutil.CaptureStdout(t, func() {
		runtime, err := New(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.Info(ctx, "event"); err != nil {
			t.Fatal(err)
		}
		if err := runtime.Close(); err != nil {
			t.Fatal(err)
		}
	})

	var got map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &got); err != nil {
		t.Fatalf("output is not one JSON line: %q: %v", output, err)
	}
	if got["severity"] != "INFO" || got["message"] != "event" {
		t.Errorf("output = %q, want the severity and message", output)
	}
	if want := "projects/my-project/traces/0af7651916cd43dd8448eb211c80319c"; got["logging.googleapis.com/trace"] != want {
		t.Errorf("trace = %v, want %q", got["logging.googleapis.com/trace"], want)
	}
	if want := "b7ad6b7169203331"; got["logging.googleapis.com/spanId"] != want {
		t.Errorf("spanId = %v, want %q", got["logging.googleapis.com/spanId"], want)
	}
}
