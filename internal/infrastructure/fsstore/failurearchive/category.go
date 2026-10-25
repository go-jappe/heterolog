package failurearchive

// FailureCategoryは、failure archiveに保存したrecordの再送可否による分類です。
// filesystem directoryとobject storage prefixの両方で同じ分類を使用します。
type FailureCategory string

const (
	FailureCategoryRetryable FailureCategory = "retryable"
	FailureCategoryPermanent FailureCategory = "permanent"
)
