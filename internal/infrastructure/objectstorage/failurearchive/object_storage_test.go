package failurearchive

import (
	"testing"

	fsfailurearchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
)

func TestObjectStoragePrefixSeparatesFailureCategories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		basePrefix      string
		failureCategory fsfailurearchive.FailureCategory
		want            string
	}{
		{
			name:            "retryable",
			basePrefix:      "backend/failures",
			failureCategory: fsfailurearchive.FailureCategoryRetryable,
			want:            "backend/failures/retryable",
		},
		{
			name:            "permanent and surrounding separators",
			basePrefix:      "/backend/failures/",
			failureCategory: fsfailurearchive.FailureCategoryPermanent,
			want:            "backend/failures/permanent",
		},
		{
			name:            "empty base",
			failureCategory: fsfailurearchive.FailureCategoryRetryable,
			want:            "retryable",
		},
		{
			name:       "empty failure category",
			basePrefix: "backend/failures",
			want:       "backend/failures",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := ObjectStoragePrefix(tt.basePrefix, tt.failureCategory); got != tt.want {
				t.Fatalf("ObjectStoragePrefix() = %q, want %q", got, tt.want)
			}
		})
	}
}
