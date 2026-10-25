package opensearch

import (
	"encoding/json"
	"errors"
	"fmt"

	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

var (
	ErrInvalidMaxBulkRequestBytes = errors.New("invalid OpenSearch max bulk request bytes")
	ErrRequestTooLarge            = errors.New("OpenSearch request is too large")
)

// ValidateMaxBulkRequestBytesは、Bulk APIの正のgzip圧縮前request body
// 上限であることを検証します。Index APIの一件送信には適用しません。
func ValidateMaxBulkRequestBytes(maxBulkRequestBytes int) error {
	if maxBulkRequestBytes <= 0 {
		return fmt.Errorf(
			"%w: value must be positive: %d",
			ErrInvalidMaxBulkRequestBytes,
			maxBulkRequestBytes,
		)
	}
	return nil
}

// BulkRecordMeasureErrorは、bulk requestへ含めるrecordの大きさを計測できなかったことを表します。
// 送信できないdocumentを渡した呼び出し側の契約errorで、原因のerrorはerrors.Is・errors.Asで辿れます。
type BulkRecordMeasureError struct {
	// InputIndexは、計測できなかったrecordの入力上の位置です。
	InputIndex int
	// Errは、計測できなかった原因です。
	Err error
}

func (e *BulkRecordMeasureError) Error() string {
	return fmt.Sprintf("measure OpenSearch bulk record at input index %d: %v", e.InputIndex, e.Err)
}

func (e *BulkRecordMeasureError) Unwrap() error { return e.Err }

// BulkRecordは、送信対象documentと呼び出し元slice上の位置を保持します。
// InputIndexは、plannerが除外したrecordおよび分割後requestの結果を元入力へ対応付ける
// ための値です。plannerは入力順を変更しません。
type BulkRecord struct {
	InputIndex int
	Document   pkgopensearch.DocumentWriteRequest
}

// RejectedBulkRecordは、backendへ送る前にapplication側のbyte上限を超過した一件です。
type RejectedBulkRecord struct {
	Record BulkRecord
	Err    error
}

// PlannedBulkRequestは、一度のIndexDocuments呼び出しで送信できるrecord群です。
// Recordsは入力順を維持し、ByteSizeは実際に送信するgzip圧縮前NDJSON bodyのbyte数です。
type PlannedBulkRequest struct {
	Records  []BulkRecord
	ByteSize int
}

// Documentsは、元入力との対応情報を除いたOpenSearch送信用sliceを返します。
// 戻り値はRecordsから新しく作るため、呼び出し元によるslice要素の置換は計画を変更しません。
func (r PlannedBulkRequest) Documents() []pkgopensearch.DocumentWriteRequest {
	documents := make([]pkgopensearch.DocumentWriteRequest, len(r.Records))
	for index, record := range r.Records {
		documents[index] = record.Document
	}
	return documents
}

// BulkPlanは、network送信前に確定したrequest群と、送信せず除外するrecord群です。
type BulkPlan struct {
	Requests []PlannedBulkRequest
	Rejected []RejectedBulkRecord
}

// DocumentBodySizeは、DocumentWriteRequest.DocをIndexDocumentsの本文行と同じJSON形式へ
// 変換したbyte数を返します。action metadataと行末の改行は含みません。
func DocumentBodySize(document pkgopensearch.DocumentWriteRequest) (int, error) {
	body, err := json.Marshal(document.Doc)
	if err != nil {
		return 0, fmt.Errorf("marshal OpenSearch document body: %w", err)
	}
	return len(body), nil
}

// PlanBulkRequestsは、一件分の完全なBulk NDJSONがrequest上限を超えるrecordを除外し、
// 残りを同じ上限内へ入力順のまま分割します。
//
// 一件だけでrequest上限を超えるrecordも、backendへ送信せずRejectedへ分類します。
// 複数件の合計だけが上限を超える場合は次のrequestへ分けます。OpenSearchや手前のproxyが
// application設定より小さい上限を持つ場合に備える413応答後の分割は、この計画とは別に
// 呼び出し側で維持します。
func PlanBulkRequests(records []BulkRecord, maxBulkRequestBytes int) (BulkPlan, error) {
	if err := ValidateMaxBulkRequestBytes(maxBulkRequestBytes); err != nil {
		return BulkPlan{}, err
	}

	plan := BulkPlan{
		Requests: make([]PlannedBulkRequest, 0, 1),
		Rejected: make([]RejectedBulkRecord, 0),
	}
	currentRecords := make([]BulkRecord, 0, len(records))
	currentBytes := 0

	flushCurrentRequest := func() {
		if len(currentRecords) == 0 {
			return
		}
		plan.Requests = append(plan.Requests, PlannedBulkRequest{
			Records:  currentRecords,
			ByteSize: currentBytes,
		})
		currentRecords = make([]BulkRecord, 0, len(records)-len(currentRecords))
		currentBytes = 0
	}

	for _, record := range records {
		recordBytes, err := pkgopensearch.BulkRequestSize(
			[]pkgopensearch.DocumentWriteRequest{record.Document},
		)
		if err != nil {
			return BulkPlan{}, &BulkRecordMeasureError{InputIndex: record.InputIndex, Err: err}
		}
		if recordBytes > maxBulkRequestBytes {
			plan.Rejected = append(plan.Rejected, RejectedBulkRecord{
				Record: record,
				Err: fmt.Errorf(
					"%w: bulk record at input index %d requires %d bytes, limit is %d",
					ErrRequestTooLarge,
					record.InputIndex,
					recordBytes,
					maxBulkRequestBytes,
				),
			})
			continue
		}

		if currentBytes > maxBulkRequestBytes-recordBytes {
			flushCurrentRequest()
		}
		currentRecords = append(currentRecords, record)
		currentBytes += recordBytes
	}
	flushCurrentRequest()

	return plan, nil
}
