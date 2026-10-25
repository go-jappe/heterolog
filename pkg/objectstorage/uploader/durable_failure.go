package uploader

import "time"

// FailureCategoryは、一回のupload後に判定した失敗性質を表します。
// fileArchiveとspoolのどちらへ保存するかは独立したdurable type設定で
// 決まり、この分類はspoolの再送可否とarchive内の診断区分にだけ使用します。
type FailureCategory string

const (
	// FailureCategoryRetryableは、compressed log fileを保持して将来再送する失敗です。
	FailureCategoryRetryable FailureCategory = "retryable"
	// FailureCategoryPermanentは、同じrequestの自動再送では解消しないことを
	// 確認できた失敗です。compressed log file自体の破損を表すquarantineとは区別します。
	FailureCategoryPermanent FailureCategory = "permanent"
)

// IsValidは定義済みのupload失敗分類かを返します。失敗分類がない空文字は含めません。
func (category FailureCategory) IsValid() bool {
	switch category {
	case FailureCategoryRetryable, FailureCategoryPermanent:
		return true
	default:
		return false
	}
}

// DurableUploadFailureDecisionは、compressed log fileの失敗性質と将来の再送時期を表します。
// Reasonは診断用の安定した文字列、Codeは取得できたHTTP status、RetryAfterは
// serviceが返した次回試行までの待機時間です。
type DurableUploadFailureDecision struct {
	FailureCategory FailureCategory
	Reason          string
	Code            int
	RetryAfter      time.Duration
}
