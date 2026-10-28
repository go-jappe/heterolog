// Package telemetryは、loggerが記録するmetricsとspan属性の名前と、記録の共通処理を
// まとめます。名前は利用者のdashboardやalertが参照するため、変えたらdocs/reference.mdと
// docs/reference.ja.mdの一覧も直します。
package telemetry

// metricsの名前です。Prometheusの慣習に合わせ、単位を名前の末尾に付けます。
const (
	// FileDroppedEntriesは、file loggerがoverloadの方針dropで捨てたentryの数です。
	FileDroppedEntries = "heterolog_file_dropped_entries_total"

	// BackendRecordsは、出力先へ送ったrecordの数を、送信の結果（outcome）ごとに数えます。
	// 失敗の分類は、failure handlerへ渡すときの分類と同じです。Slackは、送れなかった
	// recordを次のbatchで送り直すため、送るたびに数えます。bufferが一杯で捨てたrecordは
	// droppedとして数えます。
	BackendRecords = "heterolog_backend_records_total"
	// BackendRequestDurationは、出力先への1回の送信にかかった秒数です。client wrapperの
	// retryと、その間の待ち時間を含みます。
	BackendRequestDuration = "heterolog_backend_request_duration_seconds"
	// BackendFlushRecordsとBackendFlushBytesは、bufferから切り離して送るbatchの件数と
	// byte数です。byte数は、loggerがBufferFlushBytesと比べるために数えた値で、byte数の
	// 閾値を持たないSlackは記録しません。
	BackendFlushRecords = "heterolog_backend_flush_records"
	BackendFlushBytes   = "heterolog_backend_flush_bytes"
	// BackendRetriesは、client wrapperが送信をやり直した回数を、やり直す理由（reason）
	// ごとに数えます。
	BackendRetries = "heterolog_backend_retries_total"

	// CircuitBreakerRejectedRequestsは、circuit breakerが開いている間に、送らずに断った
	// 送信の数です。
	CircuitBreakerRejectedRequests = "heterolog_circuit_breaker_rejected_requests_total"
	// CircuitBreakerTransitionsは、circuit breakerが開いた回数（state=open）と、
	// 復旧を確認して閉じた回数（state=closed）です。
	CircuitBreakerTransitions = "heterolog_circuit_breaker_transitions_total"
)

// metricsの属性の名前です。
const (
	AttrBackend = "backend"
	AttrOutcome = "outcome"
	AttrReason  = "reason"
	AttrState   = "state"
)

// state属性の値です。
const (
	StateOpen   = "open"
	StateClosed = "closed"
)

// outcome属性の値です。
const (
	OutcomeSuccess    = "success"
	OutcomeFailure    = "failure"
	OutcomeRetryable  = "retryable"
	OutcomeDead       = "dead"
	OutcomeQuarantine = "quarantine"
	OutcomeDropped    = "dropped"
)

// spanの属性の名前です。spanの属性は、利用者のapplicationの属性と混ざらないよう
// heterolog.で始めます。
const (
	SpanAttrRecords = "heterolog.records"
	SpanAttrBytes   = "heterolog.bytes"
	SpanAttrAttempt = "heterolog.attempt"
)
