package uploader

import (
	"errors"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// ErrInvalidUploadReportは、要求した宛先とupload結果が一対一に対応しないことを表します。
var ErrInvalidUploadReport = errors.New("object storage destination upload report is invalid")

// IndexUploadReportは要求宛先集合に対する結果を検証し、宛先IDから参照できる形で返します。
// requested内の重複は同じ宛先として扱います。要求自体の重複を禁止する呼び出し元は
// 別途検証してください。結果の順序は問いませんが、各宛先の結果は一件だけ必要です。
// 契約違反時も要求宛先の最初の結果を返します。部分結果を採用するか、全体を拒否するかは
// 呼び出し元が決めます。各DestinationResult.Errはupload自体の成否なので検証errorには含めません。
// 契約違反は違反ごとの*UploadReportErrorで、errors.Joinでまとめて返します。
func IndexUploadReport(
	requested []objectstorage.DestinationID,
	report UploadReport,
) (map[objectstorage.DestinationID]DestinationResult, error) {
	expected := make(map[objectstorage.DestinationID]struct{}, len(requested))
	for _, destinationID := range requested {
		expected[destinationID] = struct{}{}
	}
	indexed := make(map[objectstorage.DestinationID]DestinationResult, len(report.Results))
	var errs []error
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
	for _, destinationID := range requested {
		if _, found := indexed[destinationID]; !found {
			errs = append(errs, &UploadReportError{Destination: destinationID, Err: ErrMissingDestinationResult})
		}
	}
	return indexed, errors.Join(errs...)
}
