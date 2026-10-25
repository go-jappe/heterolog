package failurearchive

import (
	"errors"
	"testing"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestFileArchiveLayoutRejectsColon(t *testing.T) {
	for _, field := range []string{"target", "file name format", "file date format"} {
		t.Run(field, func(t *testing.T) {
			cfg := newValidTestFileArchiveConfig(t)
			switch field {
			case "target":
				cfg.target = "app:stream"
			case "file name format":
				cfg.fileNameFormat += ":stream"
			case "file date format":
				cfg.fileDateFormat += "15:04"
			}
			err := ValidateLayoutConfigProvider(cfg)
			var componentErr *pkgfsstore.LogFileNameComponentError
			if !errors.As(err, &componentErr) || componentErr.Field != field {
				t.Fatalf("error = %v, want colon rejection for %s", err, field)
			}
		})
	}
}

func TestFileArchiveLayoutAllowsDrivePathAndLiteralTarget(t *testing.T) {
	cfg := newValidTestFileArchiveConfig(t)
	cfg.target = "app[1]"
	cfg.archivePath = `C:\archives`
	if err := ValidateLayoutConfigProvider(cfg); err != nil {
		t.Fatalf("drive colon and literal [] must be allowed: %v", err)
	}
}
