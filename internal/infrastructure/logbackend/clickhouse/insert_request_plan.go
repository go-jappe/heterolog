package clickhouse

import (
	"errors"
	"fmt"
	"time"

	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
)

var (
	ErrInvalidMaxInsertRequestBytes = errors.New("invalid ClickHouse max INSERT request bytes")
	ErrLogRecordTooLarge            = errors.New("ClickHouse log record exceeds max INSERT request bytes")
)

// LogRecordTooLargeErrorは一件のJSONEachRowが単独でhard limitを超えたことを表します。
// Bytesはrow末尾のnewlineを含む未圧縮byte数です。
type LogRecordTooLargeError struct {
	InputIndex int
	Bytes      int
	MaxBytes   int
}

func (e *LogRecordTooLargeError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf(
		"%v: input_index=%d bytes=%d max_bytes=%d",
		ErrLogRecordTooLarge,
		e.InputIndex,
		e.Bytes,
		e.MaxBytes,
	)
}

func (e *LogRecordTooLargeError) Unwrap() error {
	return ErrLogRecordTooLarge
}

// PlannedInsertRequestは一回の物理HTTP INSERTへ渡すrecordと、その元入力位置を保持します。
// UncompressedBytesはJSONEachRowの各row末尾newlineを含む正確なbody byte数です。
type PlannedInsertRequest struct {
	InputIndexes      []int
	Records           []pkgclickhouse.LogRecord
	UncompressedBytes int
}

// RejectedInsertRecordはHTTP送信前にJSON encoding不能または単一row超過と判定した入力です。
type RejectedInsertRecord struct {
	InputIndex        int
	UncompressedBytes int
	Err               error
}

// InsertRequestPlanはhard limit内の物理INSERT群と、送信前にlocal invalidとした入力を分けます。
type InsertRequestPlan struct {
	Requests []PlannedInsertRequest
	Rejected []RejectedInsertRecord
}

// PlanInsertRequestsは未圧縮JSONEachRowを一件ずつ実際のencoderで生成し、各物理requestが
// maxBytes以下になるよう入力順を維持して分割します。一件だけで上限を超えるrecordや
// JSONへ変換できないrecordは送信せずRejectedへ分離します。
func PlanInsertRequests(
	records []pkgclickhouse.LogRecord,
	maxBytes int,
) (InsertRequestPlan, error) {
	if maxBytes <= 0 {
		return InsertRequestPlan{}, fmt.Errorf(
			"%w: got %d, want greater than zero",
			ErrInvalidMaxInsertRequestBytes,
			maxBytes,
		)
	}

	plan := InsertRequestPlan{
		Requests: make([]PlannedInsertRequest, 0, len(records)),
		Rejected: make([]RejectedInsertRecord, 0),
	}
	current := PlannedInsertRequest{}

	appendCurrent := func() {
		if len(current.Records) == 0 {
			return
		}
		plan.Requests = append(plan.Requests, current)
		current = PlannedInsertRequest{}
	}

	defaultTimestamp := time.Now().UTC()
	for inputIndex, record := range records {
		// pkg/clickhouseもzero timestampを送信時刻で補います。計画後の再marshalで
		// timestamp文字列表現が変わらないよう、planner側で一度だけ確定します。
		if record.Timestamp.IsZero() {
			record.Timestamp = defaultTimestamp
		}
		encoded, err := pkgclickhouse.MarshalJSONEachRow([]pkgclickhouse.LogRecord{record})
		if err != nil {
			plan.Rejected = append(plan.Rejected, RejectedInsertRecord{
				InputIndex: inputIndex,
				Err:        fmt.Errorf("encode ClickHouse log record %d: %w", inputIndex, err),
			})
			continue
		}
		rowBytes := len(encoded)
		if rowBytes > maxBytes {
			plan.Rejected = append(plan.Rejected, RejectedInsertRecord{
				InputIndex:        inputIndex,
				UncompressedBytes: rowBytes,
				Err: &LogRecordTooLargeError{
					InputIndex: inputIndex,
					Bytes:      rowBytes,
					MaxBytes:   maxBytes,
				},
			})
			continue
		}

		if current.UncompressedBytes > 0 &&
			current.UncompressedBytes+rowBytes > maxBytes {
			appendCurrent()
		}
		current.InputIndexes = append(current.InputIndexes, inputIndex)
		current.Records = append(current.Records, record)
		current.UncompressedBytes += rowBytes
	}
	appendCurrent()
	return plan, nil
}
