// Package s3は、圧縮ログspoolの再送とS3固有のcircuit breaker分類を提供します。
package s3

import (
	"context"
	"errors"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infras3 "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/s3"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/worker"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// Uploaderは既存のS3送信先へ一度だけ委譲します。再試行やspoolへの保存は行いません。
// object key・送信期限などの設定は、構築済みのnextをそのまま使用します。
type Uploader struct {
	next uploader.DestinationUploader
}

var _ worker.Uploader = (*Uploader)(nil)

func New(next uploader.DestinationUploader) (*Uploader, error) {
	if next == nil {
		return nil, errors.New("S3 spool uploader requires a destination uploader")
	}
	return &Uploader{next: next}, nil
}

func (u *Uploader) Upload(ctx context.Context, input uploader.UploadRequest) error {
	return u.next.Upload(ctx, input)
}

// UploadDestinationsは送信先のuploaderが判定した失敗分類を含め、結果をそのまま返します。
func (u *Uploader) UploadDestinations(ctx context.Context, input uploader.UploadRequest, ids []objectstorage.DestinationID) uploader.UploadReport {
	return u.next.UploadDestinations(ctx, input, ids)
}

func (u *Uploader) DestinationIDs() []objectstorage.DestinationID {
	return u.next.DestinationIDs()
}

func (*Uploader) ClassifyRetry(ctx context.Context, err error) retryable.RetryDecision {
	return infras3.ClassifyRetry(ctx, err)
}
