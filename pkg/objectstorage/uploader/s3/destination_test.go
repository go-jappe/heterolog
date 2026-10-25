package s3

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestResolveDestinationIDIsStableForEquivalentDestinationSettings(t *testing.T) {
	t.Parallel()

	base := objectstorage.S3Config{
		Region:          "AP-NORTHEAST-1",
		Endpoint:        "HTTPS://S3.EXAMPLE.COM/archive///",
		AccessKeyID:     "first-access-key",
		SecretAccessKey: "first-secret-key",
		SessionToken:    "first-session-token",
		Bucket:          " archive-bucket ",
		Prefix:          "/logs/default/",
		ForcePathStyle:  false,
		Timeout:         time.Second,
	}
	want, err := ResolveDestinationID(base)
	if err != nil {
		t.Fatalf("ResolveDestinationID() error = %v", err)
	}
	if !strings.HasPrefix(string(want), "s3-") {
		t.Fatalf("destination id = %q, want s3 prefix", want)
	}

	equivalent := base
	// regionの大文字小文字と前後の空白だけが異なる設定は同じ保存先です。
	// Newは前後の空白だけを除いた入力値で署名します。
	equivalent.Region = " ap-northeast-1 "
	equivalent.Endpoint = "https://s3.example.com/archive"
	equivalent.Bucket = "archive-bucket"
	equivalent.Prefix = "logs/default"
	equivalent.AccessKeyID = "rotated-access-key"
	equivalent.SecretAccessKey = "rotated-secret-key"
	equivalent.SessionToken = "rotated-session-token"
	equivalent.ForcePathStyle = true
	equivalent.Timeout = 30 * time.Second

	got, err := ResolveDestinationID(equivalent)
	if err != nil {
		t.Fatalf("ResolveDestinationID(equivalent) error = %v", err)
	}
	if got != want {
		t.Fatalf("equivalent destination id = %q, want %q", got, want)
	}
}

func TestResolveDestinationIDChangesWithPhysicalDestination(t *testing.T) {
	t.Parallel()

	base := objectstorage.S3Config{
		Region:   "ap-northeast-1",
		Endpoint: "https://s3.example.com",
		Bucket:   "archive-bucket",
		Prefix:   "logs/default",
	}
	baseID, err := ResolveDestinationID(base)
	if err != nil {
		t.Fatalf("ResolveDestinationID() error = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*objectstorage.S3Config)
	}{
		{name: "endpoint", mutate: func(cfg *objectstorage.S3Config) { cfg.Endpoint = "https://other.example.com" }},
		{name: "region", mutate: func(cfg *objectstorage.S3Config) { cfg.Region = "us-east-1" }},
		{name: "bucket", mutate: func(cfg *objectstorage.S3Config) { cfg.Bucket = "other-bucket" }},
		{name: "effective prefix", mutate: func(cfg *objectstorage.S3Config) { cfg.Prefix = "logs/error" }},
	}
	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			changed := base
			testCase.mutate(&changed)
			changedID, err := ResolveDestinationID(changed)
			if err != nil {
				t.Fatalf("ResolveDestinationID() error = %v", err)
			}
			if changedID == baseID {
				t.Fatalf("changed destination id = %q, must differ from %q", changedID, baseID)
			}
		})
	}
}

func TestResolveDestinationIDRejectsInvalidEndpoint(t *testing.T) {
	t.Parallel()

	_, err := ResolveDestinationID(objectstorage.S3Config{
		Region:   "ap-northeast-1",
		Endpoint: "s3.example.com",
		Bucket:   "archive-bucket",
	})
	if !errors.Is(err, ErrInvalidEndpoint) {
		t.Fatalf("ResolveDestinationID() error = %v, want %v", err, ErrInvalidEndpoint)
	}
}

// clientはendpointのqueryをすべてのPUTへ付与してしまうため、宛先IDでもqueryを
// canonical化して受理せず、ValidateConfigと同じく拒否します。
func TestResolveDestinationIDRejectsEndpointQueryAndFragment(t *testing.T) {
	t.Parallel()

	for _, endpoint := range []string{
		"https://s3.example.com/?b=2&a=1",
		"https://s3.example.com/archive?",
		"https://s3.example.com/archive#fragment",
	} {
		_, err := ResolveDestinationID(objectstorage.S3Config{
			Region:   "ap-northeast-1",
			Endpoint: endpoint,
			Bucket:   "archive-bucket",
		})
		if !errors.Is(err, ErrInvalidEndpoint) {
			t.Fatalf("ResolveDestinationID(%q) error = %v, want %v", endpoint, err, ErrInvalidEndpoint)
		}
	}
}
