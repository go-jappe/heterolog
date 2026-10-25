package lifecycle

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestUploadReportIndexPreservesPartialResultsAndSentinel(t *testing.T) {
	t.Parallel()
	uploadErr := errors.New("upload failed")
	result := uploader.DestinationResult{DestinationID: "first", Err: uploadErr}
	indexed, err := validateAndIndexUploadReportForDestinations(
		[]objectstorage.DestinationID{"first", "second"},
		uploader.UploadReport{Results: []uploader.DestinationResult{result}},
	)
	require.ErrorIs(t, err, ErrInvalidUploadReport)
	require.ErrorIs(t, err, uploader.ErrInvalidUploadReport)
	require.NotErrorIs(t, err, uploadErr)
	require.Equal(t, map[objectstorage.DestinationID]uploader.DestinationResult{"first": result}, indexed)
}

func TestUploadReportIndexTreatsRequestedDestinationsAsSet(t *testing.T) {
	t.Parallel()
	result := uploader.DestinationResult{DestinationID: "first"}
	indexed, err := validateAndIndexUploadReportForDestinations(
		[]objectstorage.DestinationID{"first", "first"},
		uploader.UploadReport{Results: []uploader.DestinationResult{result}},
	)
	require.NoError(t, err)
	require.Equal(t, map[objectstorage.DestinationID]uploader.DestinationResult{"first": result}, indexed)
}
