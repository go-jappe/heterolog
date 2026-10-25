package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestProcessingFileTransitionRetry(
	t testing.TB,
	cfg ProcessingFileTransitionRetryConfig,
) *ProcessingFileTransitionRetry {
	t.Helper()
	retry, err := NewProcessingFileTransitionRetry(NewProcessingFileTransitionRetryConfigAdapter(cfg))
	require.NoError(t, err)
	return retry
}
