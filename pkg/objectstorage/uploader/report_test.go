package uploader

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestIndexUploadReport(t *testing.T) {
	t.Parallel()
	uploadErr := errors.New("remote upload failed")
	for _, test := range []struct {
		name      string
		requested []objectstorage.DestinationID
		results   []DestinationResult
		want      map[objectstorage.DestinationID]DestinationResult
		// wantErrは、報告される契約違反を報告順に並べたものです。
		wantErr []UploadReportError
	}{
		{
			name: "empty",
			want: map[objectstorage.DestinationID]DestinationResult{},
		},
		{
			name:      "out of order results retain upload failures",
			requested: []objectstorage.DestinationID{"first", "second"},
			results: []DestinationResult{
				{DestinationID: "second", Err: uploadErr},
				{DestinationID: "first"},
			},
			want: map[objectstorage.DestinationID]DestinationResult{
				"first":  {DestinationID: "first"},
				"second": {DestinationID: "second", Err: uploadErr},
			},
		},
		{
			name:      "missing result retains classified destinations",
			requested: []objectstorage.DestinationID{"first", "second"},
			results:   []DestinationResult{{DestinationID: "first"}},
			want: map[objectstorage.DestinationID]DestinationResult{
				"first": {DestinationID: "first"},
			},
			wantErr: []UploadReportError{{Destination: "second", Err: ErrMissingDestinationResult}},
		},
		{
			name:      "duplicate result preserves first failure",
			requested: []objectstorage.DestinationID{"first"},
			results: []DestinationResult{
				{DestinationID: "first", Err: uploadErr},
				{DestinationID: "first"},
			},
			want: map[objectstorage.DestinationID]DestinationResult{
				"first": {DestinationID: "first", Err: uploadErr},
			},
			wantErr: []UploadReportError{{Destination: "first", Err: ErrDuplicateDestinationResult}},
		},
		{
			name:      "multiple contract violations preserve first success",
			requested: []objectstorage.DestinationID{"first", "second"},
			results: []DestinationResult{
				{DestinationID: "unknown"},
				{DestinationID: "first"},
				{DestinationID: "first", Err: uploadErr},
			},
			want: map[objectstorage.DestinationID]DestinationResult{
				"first": {DestinationID: "first"},
			},
			wantErr: []UploadReportError{
				{Destination: "unknown", Index: 0, Err: ErrUnrequestedDestinationResult},
				{Destination: "first", Err: ErrDuplicateDestinationResult},
				{Destination: "second", Err: ErrMissingDestinationResult},
			},
		},
		{
			name:      "requested destinations are a set",
			requested: []objectstorage.DestinationID{"first", "first"},
			results:   []DestinationResult{{DestinationID: "first"}},
			want: map[objectstorage.DestinationID]DestinationResult{
				"first": {DestinationID: "first"},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			indexed, err := IndexUploadReport(test.requested, UploadReport{Results: test.results})
			require.Equal(t, test.want, indexed)
			if len(test.wantErr) == 0 {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrInvalidUploadReport)
			require.NotErrorIs(t, err, uploadErr)
			require.Equal(t, test.wantErr, uploadReportErrors(err))
		})
	}
}

// uploadReportErrorsは、errors.Joinでまとめたerrに含まれるUploadReportErrorを順に返します。
func uploadReportErrors(err error) []UploadReportError {
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		return nil
	}
	var found []UploadReportError
	for _, child := range joined.Unwrap() {
		var reportErr *UploadReportError
		if errors.As(child, &reportErr) {
			found = append(found, *reportErr)
		}
	}
	return found
}

// UploadReportErrorとDestinationUploadErrorの表示形式は、operatorがどの宛先の結果を
// 調べるかを読むerror文字列なので、型ごとに一度だけ文字列で固定します。
func TestUploadErrorFormats(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		err  error
		want string
	}{
		{
			err: &UploadReportError{Destination: "unknown", Index: 2, Err: ErrUnrequestedDestinationResult},
			want: `object storage destination upload report is invalid: ` +
				`result[2] contains unrequested destination "unknown"`,
		},
		{
			err:  &UploadReportError{Destination: "first", Err: ErrDuplicateDestinationResult},
			want: `object storage destination upload report is invalid: destination "first" is classified more than once`,
		},
		{
			err:  &UploadReportError{Destination: "second", Err: ErrMissingDestinationResult},
			want: `object storage destination upload report is invalid: destination "second" is not classified`,
		},
		{
			err:  &DestinationUploadError{Destination: "s3:archive", Err: errors.New("remote upload failed")},
			want: `object storage destination "s3:archive" failed: remote upload failed`,
		},
	} {
		require.EqualError(t, test.err, test.want)
	}
}

func TestUploadReportErrIdentifiesEachFailedDestination(t *testing.T) {
	t.Parallel()
	firstErr := errors.New("first failed")
	thirdErr := errors.New("third failed")
	report := UploadReport{Results: []DestinationResult{
		{DestinationID: "first", Err: firstErr},
		{DestinationID: "second"},
		{DestinationID: "third", Err: thirdErr},
	}}

	err := report.Err()
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, thirdErr)
	joined, ok := err.(interface{ Unwrap() []error })
	require.True(t, ok, "report error must join one error per failed destination")
	var got []DestinationUploadError
	for _, child := range joined.Unwrap() {
		var destinationErr *DestinationUploadError
		require.ErrorAs(t, child, &destinationErr)
		got = append(got, *destinationErr)
	}
	require.Equal(t, []DestinationUploadError{
		{Destination: "first", Err: firstErr},
		{Destination: "third", Err: thirdErr},
	}, got)
	require.NoError(t, UploadReport{Results: []DestinationResult{{DestinationID: "first"}}}.Err())
}
