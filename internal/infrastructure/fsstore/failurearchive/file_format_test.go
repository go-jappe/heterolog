package failurearchive

import (
	"strings"
	"testing"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestFileFormatContractUsesSharedJSONLFormat(t *testing.T) {
	t.Parallel()

	if FileNameFormat != pkgfsstore.FailureArchiveFileNameFormat {
		t.Fatalf(
			"FileNameFormat = %q, want pkg/fsstore failure archive format %q",
			FileNameFormat,
			pkgfsstore.FailureArchiveFileNameFormat,
		)
	}
	if !strings.HasSuffix(FileNameFormat, "."+FileExtension) {
		t.Fatalf(
			"FileNameFormat = %q, want extension %q",
			FileNameFormat,
			FileExtension,
		)
	}
}
