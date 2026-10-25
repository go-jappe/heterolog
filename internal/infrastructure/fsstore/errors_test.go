package fsstore

import (
	"errors"
	"testing"
)

// requireFileRemovalStepは、errがstepの段階で失敗したFileRemovalErrorをwrapし、
// そのpathがpathであることを確認します。段階にpathがない場合はpathに空文字を渡します。
func requireFileRemovalStep(t testing.TB, err error, step FileRemovalStep, path string) {
	t.Helper()
	var removal *FileRemovalError
	if !errors.As(err, &removal) || removal.Step != step || removal.Path != path {
		t.Fatalf("error = %v, want FileRemovalError{Step: %q, Path: %q}", err, step, path)
	}
}

// requireFileRemovalFailureは、JoinFileRemovalFailuresのerrが、fileの観測情報を持つ
// FileRemovalFailureをwrapすることを確認します。
func requireFileRemovalFailure(t testing.TB, err error, file FileInfo) {
	t.Helper()
	var failure *FileRemovalFailure
	if !errors.As(err, &failure) || failure.File.Path != file.Path || !failure.File.ModTime.Equal(file.ModTime) {
		t.Fatalf("joined error = %v, want FileRemovalFailure for %+v", err, file)
	}
}
