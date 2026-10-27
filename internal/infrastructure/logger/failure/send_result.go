// Package failureは、loggerの送信からdurable failure handlerへ渡す、
// backendに依存しない結果を定義します。
package failure

// SendResultは、送信のterminalな結果を示す証拠と、durableな処理が必要な
// recordを保持します。失敗した各recordには、backendの結果が判明した時点で、
// 処理のdispositionがちょうど1つ割り当てられます。
//
//   - RetryableRecordsは、保存したrecordを変更せずに再送できます。
//   - DeadRecordsは、構造的に信頼でき、terminalなdelivery結果が確定した
//     recordです。
//   - QuarantineRecordsは、record、そのbackendでの表現、またはその結果を
//     調査・修復しないまま、自動でreplayしてはいけません。
//
// FailureCodeとFailureReasonは、recordがそのdispositionになった理由を保持します。
// durable handlerは確定済みの分類をそのまま使い、backend固有のfailure codeを
// 解釈し直しません。
type SendResult struct {
	Meta              Meta
	TerminalError     error
	RetryableRecords  []FailedRecord
	DeadRecords       []FailedRecord
	QuarantineRecords []FailedRecord
}

// HasFailedRecordsは、durableに処理すべきpayload dataがあるかを返します。
func (r SendResult) HasFailedRecords() bool {
	return len(r.RetryableRecords) > 0 || len(r.DeadRecords) > 0 || len(r.QuarantineRecords) > 0
}

// HasSendFailureは、送信のterminal failure、または失敗recordの証拠があるかを返します。
func (r SendResult) HasSendFailure() bool {
	return r.TerminalError != nil || r.HasFailedRecords()
}

// FailedCountは、durableな処理が必要なrecordの数を返します。deliveryが
// 未確認のrecordとlocal rejectionのrecordも含みます。
func (r SendResult) FailedCount() int {
	return len(r.RetryableRecords) + len(r.DeadRecords) + len(r.QuarantineRecords)
}

// SuccessCountは、成功が確定したinputの数を返します。terminalな送信errorが
// ある場合は、一覧にないinputも成功とはみなせません。
func (r SendResult) SuccessCount() int {
	if r.TerminalError != nil || r.Meta.InputCount <= 0 {
		return 0
	}

	failed := r.FailedCount()
	if failed >= r.Meta.InputCount {
		return 0
	}
	return r.Meta.InputCount - failed
}
