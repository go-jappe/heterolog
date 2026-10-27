package cloudwatchlogs

import (
	"errors"
	"strings"
	"testing"
	"time"

	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

func TestResolveLogStreamName(t *testing.T) {
	t.Parallel()

	location := time.FixedZone("JST", 9*60*60)
	at := time.Date(2026, time.August, 13, 17, 42, 0, 0, location)
	tests := []struct {
		name        string
		granularity pkgcloudwatchlogs.StreamRouting
		want        string
	}{
		{name: "zero value uses fixed stream", want: "app/task-1"},
		{name: "none", granularity: pkgcloudwatchlogs.StreamRoutingNone, want: "app/task-1"},
		{name: "monthly", granularity: pkgcloudwatchlogs.StreamRoutingMonthly, want: "app/task-1/2026/08"},
		{name: "daily", granularity: pkgcloudwatchlogs.StreamRoutingDaily, want: "app/task-1/2026/08/13"},
		{name: "hourly", granularity: pkgcloudwatchlogs.StreamRoutingHourly, want: "app/task-1/2026/08/13/17"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ResolveLogStreamName("app/task-1", tt.granularity, at)
			if err != nil {
				t.Fatalf("ResolveLogStreamName() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("ResolveLogStreamName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveLogStreamNameUsesProvidedTimeLocation(t *testing.T) {
	t.Parallel()

	instantUTC := time.Date(2026, time.August, 31, 15, 30, 0, 0, time.UTC)
	location := time.FixedZone("JST", 9*60*60)

	utcName, err := ResolveLogStreamName(
		"app",
		pkgcloudwatchlogs.StreamRoutingDaily,
		instantUTC,
	)
	if err != nil {
		t.Fatalf("ResolveLogStreamName(UTC) error = %v", err)
	}
	localName, err := ResolveLogStreamName(
		"app",
		pkgcloudwatchlogs.StreamRoutingDaily,
		instantUTC.In(location),
	)
	if err != nil {
		t.Fatalf("ResolveLogStreamName(JST) error = %v", err)
	}
	if utcName != "app/2026/08/31" {
		t.Fatalf("UTC stream = %q, want %q", utcName, "app/2026/08/31")
	}
	if localName != "app/2026/09/01" {
		t.Fatalf("JST stream = %q, want %q", localName, "app/2026/09/01")
	}
}

func TestResolveLogStreamNameRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		baseName      string
		granularity   pkgcloudwatchlogs.StreamRouting
		wantErr       error
		wantCharacter rune
	}{
		{name: "empty base", baseName: " ", wantErr: pkgcloudwatchlogs.ErrLogStreamNameRequired},
		{name: "unsupported granularity", baseName: "app", granularity: "weekly", wantErr: pkgcloudwatchlogs.ErrInvalidStreamRouting},
		{name: "forbidden character", baseName: "app:*", wantErr: pkgcloudwatchlogs.ErrLogStreamNameInvalidCharacter},
		{name: "embedded newline", baseName: "app\nworker", wantErr: ErrLogStreamNameControlCharacter, wantCharacter: '\n'},
		{name: "invisible format character", baseName: "app\u200bworker", wantErr: ErrLogStreamNameFormatCharacter, wantCharacter: '\u200b'},
		{name: "trailing whitespace", baseName: "app/worker ", wantErr: ErrLogStreamNameSurroundingWhitespace},
		{
			// suffixを付けた後の名前でAWSの文字数上限を検証します。
			name:        "routed name exceeds AWS limit",
			baseName:    strings.Repeat("a", 512),
			granularity: pkgcloudwatchlogs.StreamRoutingHourly,
			wantErr:     pkgcloudwatchlogs.ErrLogStreamNameTooLong,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ResolveLogStreamName(
				tt.baseName,
				tt.granularity,
				time.Date(2026, time.August, 13, 17, 0, 0, 0, time.UTC),
			)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ResolveLogStreamName() error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantCharacter != 0 {
				var characterErr *LogStreamNameCharacterError
				if !errors.As(err, &characterErr) || characterErr.Character != tt.wantCharacter {
					t.Fatalf("ResolveLogStreamName() error = %#v, want rejected character %U", err, tt.wantCharacter)
				}
			}
		})
	}
	// 基準名だけなら上限内でも、suffixを付けた名前が上限を超える場合に拒否します。
	if _, err := ResolveLogStreamName(strings.Repeat("a", 512), pkgcloudwatchlogs.StreamRoutingNone, time.Date(2026, time.August, 13, 17, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("ResolveLogStreamName() without suffix error = %v, want nil", err)
	}
}

func TestLogStreamNameResolverUsesEventTimezone(t *testing.T) {
	t.Parallel()
	resolver, err := NewLogStreamNameResolver("container/task-1", pkgcloudwatchlogs.StreamRoutingHourly)
	if err != nil {
		t.Fatal(err)
	}
	utcTime := time.Date(2026, time.August, 13, 15, 30, 0, 0, time.UTC)
	for _, test := range []struct {
		name string
		at   time.Time
		want string
	}{
		{"UTC", utcTime, "container/task-1/2026/08/13/15"},
		{"JST", utcTime.In(time.FixedZone("JST", 9*60*60)), "container/task-1/2026/08/14/00"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolver.ResolveLogStreamNameForEvent(test.at)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("streamName = %q, want %q", got, test.want)
			}
		})
	}
}

func TestLogStreamNameResolverRejectsZeroEventTime(t *testing.T) {
	t.Parallel()

	resolver, err := NewLogStreamNameResolver(
		"app",
		pkgcloudwatchlogs.StreamRoutingNone,
	)
	if err != nil {
		t.Fatalf("NewLogStreamNameResolver() error = %v", err)
	}

	_, err = resolver.ResolveLogStreamNameForEvent(time.Time{})
	if !errors.Is(err, ErrEventTimeRequired) {
		t.Fatalf("ResolveLogStreamNameForEvent() error = %v, want ErrEventTimeRequired", err)
	}
}

func TestNewLogStreamNameResolverRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		baseName    string
		granularity pkgcloudwatchlogs.StreamRouting
		wantErr     error
	}{
		{name: "empty base name", baseName: " ", wantErr: pkgcloudwatchlogs.ErrLogStreamNameRequired},
		{name: "invalid granularity", baseName: "app", granularity: "weekly", wantErr: pkgcloudwatchlogs.ErrInvalidStreamRouting},
		{
			name:        "routed name exceeds AWS limit",
			baseName:    strings.Repeat("a", 512),
			granularity: pkgcloudwatchlogs.StreamRoutingHourly,
			wantErr:     pkgcloudwatchlogs.ErrLogStreamNameTooLong,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resolver, err := NewLogStreamNameResolver(
				tt.baseName,
				tt.granularity,
			)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("NewLogStreamNameResolver() error = %v, want %v", err, tt.wantErr)
			}
			if resolver != nil {
				t.Fatalf("NewLogStreamNameResolver() resolver = %#v, want nil", resolver)
			}
		})
	}
}
