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
	// droppedとして数えます。file loggerは、1件の書き込みごとにsuccessかfailureとして
	// 数えます。
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

	// FailureRecordsは、出力先へ送れなかったrecordを、行き先（action）ごとに数えます。
	// 同じrecordを複数の行き先（spoolとfailure archiveなど）で処理する設定では、
	// 行き先ごとに数えます。
	FailureRecords = "heterolog_failure_records_total"

	// CircuitBreakerRejectedRequestsは、circuit breakerが開いている間に、送らずに断った
	// 送信の数です。
	CircuitBreakerRejectedRequests = "heterolog_circuit_breaker_rejected_requests_total"
	// CircuitBreakerTransitionsは、circuit breakerが開いた回数（state=open）と、
	// 復旧を確認して閉じた回数（state=closed）です。
	CircuitBreakerTransitions = "heterolog_circuit_breaker_transitions_total"

	// ObjectStorageUploadsは、圧縮したログファイルなどをobject storageの1つの宛先へ
	// uploadした回数を、結果（outcome）ごとに数えます。失敗は、後で送り直すretryableと、
	// 送り直さないpermanentに分けます。
	ObjectStorageUploads = "heterolog_object_storage_uploads_total"
	// ObjectStorageUploadDurationは、1つの宛先への1回のuploadにかかった秒数です。
	ObjectStorageUploadDuration = "heterolog_object_storage_upload_duration_seconds"

	// AsyncRejectedEntriesは、非同期APIが受け付けずに捨てたentryの数を、理由（reason）
	// ごとに数えます。非同期APIは、この拒否を呼び出し元へ返しません。
	AsyncRejectedEntries = "heterolog_async_rejected_entries_total"
	// AsyncBackendFailuresは、非同期APIが出力先へ渡したentryのうち、出力先がerrorを返したり、
	// 時間内に終わらなかったり、panicしたりしたものの数を、出力先と理由（reason）ごとに
	// 数えます。非同期APIは、この失敗を呼び出し元へ返しません。
	AsyncBackendFailures = "heterolog_async_backend_failures_total"

	// MemoryPressureRatioは、出力先（backend）のspoolが起動したmemory逼迫の監視が最後に
	// 読んだmemoryの使用率（0から1）です。信頼できる値を読めた場合だけ記録します。
	MemoryPressureRatio = "heterolog_memory_pressure_ratio"
	// MemoryPressureCriticalは、同じ監視の最後の判定で、memoryが逼迫している（critical）
	// なら1、そうでなければ0です。
	MemoryPressureCritical = "heterolog_memory_pressure_critical"

	// SpoolReplayRecordsは、spoolから読み戻して送り直したrecordの数を、送り直した結果
	// （outcome）ごとに数えます。retryableは、spoolへ戻して後でまた送り直すrecordです。
	SpoolReplayRecords = "heterolog_spool_replay_records_total"

	// TraceSpansは、NewTelemetryのtraceの配送の仕組みが、送り先（exporter）ごとに扱った
	// spanの数を、結果（outcome）ごとに数えます。successは送れたspan、retryable・dead・
	// quarantineは送れずにfailure storeへ残したspan、failureは送れず残しもしなかったspan、
	// droppedは待ち行列が一杯の場合や終了の後に捨てたspanです。配送の仕組みは、自分の送信のspanを作らず、このmetricsで様子を
	// 知らせます。
	TraceSpans = "heterolog_trace_spans_total"
	// TraceExportDurationは、送り先が1つのbatchを送るのにかかった秒数です。再試行と、その
	// 間の待ち時間を含みます。
	TraceExportDuration = "heterolog_trace_export_duration_seconds"
	// TraceExportRetriesは、otlp・opensearch・clickhouseの送り先が送信をやり直した回数を、
	// やり直す理由（reason）ごとに数えます。
	TraceExportRetries = "heterolog_trace_export_retries_total"
	// MetricPointsは、NewTelemetryのmetricsの配送の仕組みが、送り先（exporter）ごとに扱った
	// data pointの数を、結果（outcome）ごとに数えます。結果の値は、TraceSpansと同じです。
	MetricPoints = "heterolog_metric_points_total"
	// MetricExportDurationは、送り先が1つのbatchを送るのにかかった秒数です。再試行と、その
	// 間の待ち時間を含みます。
	MetricExportDuration = "heterolog_metric_export_duration_seconds"
	// MetricExportRetriesは、送り先が送信をやり直した回数を、やり直す理由（reason）ごとに
	// 数えます。
	MetricExportRetries = "heterolog_metric_export_retries_total"

	// InternalLogsは、loggerが自分の状態を知らせる内部ログのうち、通知先（OnErrorConfigなどで
	// 選んだ出力先）へ渡したものの数を、level（error・warn・info・debug）ごとに数えます。
	InternalLogs = "heterolog_internal_logs_total"
)

// metricsの属性の名前です。
const (
	AttrBackend = "backend"
	AttrOutcome = "outcome"
	AttrReason  = "reason"
	AttrState   = "state"
	AttrAction  = "action"
	AttrLevel   = "level"
	// AttrTargetは、object storageの種類（s3など）です。
	AttrTarget = "target"
	// AttrExporterは、spanとmetricsの送り先（otlp・file・stdoutなど）です。
	AttrExporter = "exporter"
)

// action属性の値です。
const (
	// ActionSpooledは、spoolへ保存したrecordです。retryableのrecordは後で送り直します。
	ActionSpooled = "spooled"
	// ActionArchivedは、failure archiveへ保存したrecordです。
	ActionArchived = "archived"
	// ActionDegradedは、stdoutへ退避したrecordです。
	ActionDegraded = "degraded"
	// ActionDroppedは、overloadの方針dropで捨てたrecordです。
	ActionDropped = "dropped"
	// ActionFailedは、保存も退避もできなかったrecordです。
	ActionFailed = "failed"
)

// reason属性の値です。backendのretryの理由は、retryの分類の値を使います。
const (
	// ReasonInvalidは、検証に失敗した入力（一件の上限を超えたentryなど）です。
	ReasonInvalid = "invalid"
	// ReasonClosedは、Closeを始めた後の入力です。
	ReasonClosed = "closed"
	// ReasonErrorは、出力先が返したerrorです。
	ReasonError = "error"
	// ReasonTimeoutは、出力先が時間内に終わらなかったことです。
	ReasonTimeout = "timeout"
	// ReasonPanicは、出力先がpanicしたことです。
	ReasonPanic = "panic"
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
	OutcomePermanent  = "permanent"
)

// spanの属性の名前です。spanの属性は、利用者のapplicationの属性と混ざらないよう
// heterolog.で始めます。
const (
	SpanAttrRecords = "heterolog.records"
	SpanAttrBytes   = "heterolog.bytes"
	SpanAttrAttempt = "heterolog.attempt"
	// SpanAttrBackendは、出力先の名前です。出力先ごとに名前を分けないspanに付けます。
	SpanAttrBackend = "heterolog.backend"
	// SpanAttrTargetは、object storageの種類（s3など）です。
	SpanAttrTarget = "heterolog.target"
	// SpanAttrDestinationとSpanAttrLevelは、内部ログの通知先の名前とlevelです。
	SpanAttrDestination = "heterolog.destination"
	SpanAttrLevel       = "heterolog.level"
)
