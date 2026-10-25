package appendlog

import (
	"errors"
	"testing"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestValidateLogFileNameConfigProviderRejectsColon(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*pkgfsstore.FsstoreConfig)
	}{
		{"target", func(c *pkgfsstore.FsstoreConfig) { c.Target = "app:stream" }},
		{"error log prefix", func(c *pkgfsstore.FsstoreConfig) {
			c.IsSeparateErrorLogsEnabled = true
			c.SeparateErrorLogsPrefix = "error:"
		}},
		{"debug log prefix", func(c *pkgfsstore.FsstoreConfig) {
			c.IsSeparateDebugLogsEnabled = true
			c.SeparateDebugLogsPrefix = "debug:"
		}},
		{"file name format", func(c *pkgfsstore.FsstoreConfig) { c.FileNameFormat += ":stream" }},
		{"file date format", func(c *pkgfsstore.FsstoreConfig) { c.FileDateFormat += "15:04" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := pkgfsstore.FsstoreConfig{Target: "app", FileNameFormat: pkgfsstore.FileNameFormat, FileDateFormat: pkgfsstore.FileDateFormat}
			test.change(&cfg)
			err := ValidateLogFileNameConfigProvider(NewFsstoreConfigAdapter(cfg))
			var componentErr *pkgfsstore.LogFileNameComponentError
			if !errors.As(err, &componentErr) || componentErr.Field != test.name {
				t.Fatalf("error = %v, want field-specific colon error", err)
			}
		})
	}
}

func TestValidateLogFileNameConfigProviderAllowsDrivePathAndUnusedPrefixes(t *testing.T) {
	// pathのdrive colonと、以前対応したliteralな[]は拒否しません。
	// 未使用prefixには古い設定が残っていてもfile名に使用されません。
	cfg := pkgfsstore.FsstoreConfig{
		Target: "app[1]", LogPath: `C:\logs`, LogDir: "logs[1]", ErrorLogDir: "errors[1]",
		FileNameFormat: pkgfsstore.FileNameFormat, FileDateFormat: pkgfsstore.FileDateFormat,
		SeparateErrorLogsPrefix: "unused:error", SeparateDebugLogsPrefix: "unused:debug",
	}
	adapter := NewFsstoreConfigAdapter(cfg)
	if err := ValidateLogFileNameConfigProvider(adapter); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLayoutConfigProvider(adapter); err != nil {
		t.Fatal(err)
	}
}

func TestValidateLayoutConfigProviderRejectsColon(t *testing.T) {
	for _, change := range []func(*pkgfsstore.FsstoreConfig){
		func(c *pkgfsstore.FsstoreConfig) { c.Target = "app:stream" },
		func(c *pkgfsstore.FsstoreConfig) { c.FileNameFormat += ":stream" },
	} {
		cfg := pkgfsstore.FsstoreConfig{Target: "app", LogPath: t.TempDir(), FileNameFormat: pkgfsstore.FileNameFormat, FileDateFormat: pkgfsstore.FileDateFormat}
		change(&cfg)
		err := ValidateLayoutConfigProvider(NewFsstoreConfigAdapter(cfg))
		if !errors.Is(err, pkgfsstore.ErrInvalidLogFileNameComponent) {
			t.Fatalf("error = %v, want colon rejection", err)
		}
	}
}
