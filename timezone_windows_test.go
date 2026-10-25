//go:build windows

package heterolog

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
)

func TestApplicationTimezonesWithoutExternalData(t *testing.T) {
	const childEnv = "HETEROLOG_TIMEZONE_TEST_CHILD"
	if os.Getenv(childEnv) != "1" {
		// time packageの環境変数cacheや、他testで読み込んだtimezoneに依存しない
		// 新しいprocessで、Go付属と外部指定の両データを利用不能にします。
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		directory := t.TempDir()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, executable,
			"-test.run=^TestApplicationTimezonesWithoutExternalData$", "-test.v", "-test.timeout=20s")
		command.Env = append(os.Environ(), childEnv+"=1",
			"GOROOT="+filepath.Join(directory, "missing-go"),
			"ZONEINFO="+filepath.Join(directory, "missing-zoneinfo"))
		command.WaitDelay = time.Second
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("timezone child failed: %v (context: %v)\n%s", err, ctx.Err(), output)
		}
		t.Logf("timezone child:\n%s", output)
		return
	}

	// WindowsにはGoが探索するUnix形式のsystem zoneinfo directoryがありません。
	// GOROOT/ZONEINFOも存在しないため、成功にはheterologの埋め込みデータが必要です。
	if runtime.GOROOT() != os.Getenv("GOROOT") {
		t.Fatal("child did not use the isolated GOROOT")
	}
	for _, path := range []string{runtime.GOROOT(), os.Getenv("ZONEINFO")} {
		if path == "" {
			t.Fatal("external timezone source was not isolated")
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("external timezone source must be absent: %v", err)
		}
	}

	for _, tt := range []struct {
		name       string
		zone       string
		month      time.Month
		wantOffset int
	}{
		{"Tokyo", "Asia/Tokyo", time.January, 9 * 60 * 60},
		{"New York winter", "America/New_York", time.January, -5 * 60 * 60},
		{"New York summer", "America/New_York", time.July, -4 * 60 * 60},
	} {
		t.Run(tt.name, func(t *testing.T) {
			formatter, err := logging.NewTimestampFormatter(true, tt.zone, time.RFC3339)
			if err != nil {
				t.Fatal(err)
			}
			instant := time.Date(2026, tt.month, 15, 12, 0, 0, 0, time.UTC)
			_, offset := instant.In(formatter.Location()).Zone()
			if offset != tt.wantOffset {
				t.Fatalf("timezone %s offset=%d, want %d", tt.zone, offset, tt.wantOffset)
			}
		})
	}
	if _, err := logging.NewTimestampFormatter(true, "Invalid/Timezone", time.RFC3339); err == nil {
		t.Fatal("invalid timezone was accepted")
	}
}
