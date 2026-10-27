package failurearchive

import (
	"strings"
	"testing"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestFileFormatContractUsesSharedJSONLFormat(t *testing.T) {
	t.Parallel()

	if pkgfsstore.FailureArchiveFileNameFormat != pkgfsstore.FailureArchiveFileNameFormat {
		t.Fatalf(
			"FileNameFormat = %q, want pkg/fsstore failure archive format %q",
			pkgfsstore.FailureArchiveFileNameFormat,
			pkgfsstore.FailureArchiveFileNameFormat,
		)
	}
	if !strings.HasSuffix(pkgfsstore.FailureArchiveFileNameFormat, "."+FileExtension) {
		t.Fatalf(
			"FileNameFormat = %q, want extension %q",
			pkgfsstore.FailureArchiveFileNameFormat,
			FileExtension,
		)
	}
}
