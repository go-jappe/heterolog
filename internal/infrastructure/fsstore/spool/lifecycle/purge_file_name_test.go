package lifecycle

import "testing"

func TestValidateSpoolFileNameAcceptsWholeFileQuarantineReasonSuffix(t *testing.T) {
	t.Parallel()

	name := "56789_random-quarantine-invalid-export-report.hlogspool"
	if err := validateSpoolFileName(name); err != nil {
		t.Fatalf("validateSpoolFileName(%q) returned error: %v", name, err)
	}
}
