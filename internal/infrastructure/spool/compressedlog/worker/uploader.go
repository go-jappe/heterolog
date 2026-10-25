package worker

import (
	"context"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// Uploaderは圧縮ログspoolからの送信と、circuit breaker用の判定を担います。
// WorkerはUploadDestinationsへ一宛先だけを渡し、その送信結果のFailureCategoryで
// activeへ戻すかdeadへ移すかを決めます。送信エラーからの再分類は行いません。
type Uploader interface {
	uploader.DestinationUploader
	// ClassifyRetryはbreakerに数える失敗を判定します。ここで送信を再試行しません。
	ClassifyRetry(context.Context, error) retryable.RetryDecision
}
