package cloudwatchlogs

const (
	// ErrorTypeResourceNotFoundException は CloudWatch Logs が存在しない resource に対して返す error type です。
	ErrorTypeResourceNotFoundException = "ResourceNotFoundException"

	// ErrorTypeResourceAlreadyExistsException は CloudWatch Logs が既存 resource 作成に対して返す error type です。
	ErrorTypeResourceAlreadyExistsException = "ResourceAlreadyExistsException"

	// ErrorTypeOperationAbortedException は CloudWatch Logs が同時更新などの競合時に返す error type です。
	ErrorTypeOperationAbortedException = "OperationAbortedException"

	// ErrorTypeRequestAbortedException は、responseを返す前にrequest処理が中断された場合の
	// AWS共通error typeです。呼び出し元のcontext終了とは別に、backend responseとして
	// 受け取った場合のretry分類に使用します。
	ErrorTypeRequestAbortedException = "RequestAbortedException"
)
