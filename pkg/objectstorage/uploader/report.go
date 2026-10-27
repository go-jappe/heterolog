package uploader

import (
	"errors"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// ErrInvalidUploadReport reports that the upload results do not correspond
// one-to-one with the requested destinations.
var ErrInvalidUploadReport = errors.New("object storage destination upload report is invalid")

// IndexUploadReport validates the results against the requested destinations
// and returns them in a form that can be looked up by destination ID. A
// destination ID listed more than once in requested has no meaning and is
// likely a configuration mistake, so it is reported with
// ErrDuplicateDestinationID. The results may be in any order, but each
// destination needs exactly one result.
// Even when the contract is violated, it returns the first result for each
// requested destination; the caller decides whether to use the partial results
// or reject them all. Each DestinationResult.Err is the outcome of the upload
// itself, so it is not included in the validation error. Contract violations
// are returned as one *UploadReportError per violation, joined with
// errors.Join.
func IndexUploadReport(
	requested []objectstorage.DestinationID,
	report UploadReport,
) (map[objectstorage.DestinationID]DestinationResult, error) {
	// expectedの値は、requestedで重複を報告済みかを表します。
	expected := make(map[objectstorage.DestinationID]bool, len(requested))
	unique := make([]objectstorage.DestinationID, 0, len(requested))
	var errs []error
	for _, destinationID := range requested {
		reported, duplicate := expected[destinationID]
		if !duplicate {
			expected[destinationID] = false
			unique = append(unique, destinationID)
			continue
		}
		// 同じ宛先が何回重なっていても、宛先ごとに一度だけ報告します。
		if !reported {
			expected[destinationID] = true
			errs = append(errs, &UploadReportError{Destination: destinationID, Err: ErrDuplicateDestinationID})
		}
	}
	indexed := make(map[objectstorage.DestinationID]DestinationResult, len(report.Results))
	for index, result := range report.Results {
		if _, ok := expected[result.DestinationID]; !ok {
			errs = append(errs, &UploadReportError{
				Destination: result.DestinationID, Index: index, Err: ErrUnrequestedDestinationResult,
			})
			continue
		}
		if _, duplicate := indexed[result.DestinationID]; duplicate {
			errs = append(errs, &UploadReportError{
				Destination: result.DestinationID, Err: ErrDuplicateDestinationResult,
			})
			continue
		}
		indexed[result.DestinationID] = result
	}
	for _, destinationID := range unique {
		if _, found := indexed[destinationID]; !found {
			errs = append(errs, &UploadReportError{Destination: destinationID, Err: ErrMissingDestinationResult})
		}
	}
	return indexed, errors.Join(errs...)
}
