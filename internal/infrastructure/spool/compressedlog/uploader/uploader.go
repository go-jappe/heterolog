// Package uploaderは、保存先サービスに対応する圧縮ログspoolの再送部品を構築します。
package uploader

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/uploader/s3"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/worker"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkguploader "github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// ErrUnsupportedUploadServiceは、圧縮ログspoolの再送部品がない保存先サービスを指定したことを表します。
// 実際に返すerrorはUnsupportedUploadServiceErrorです。
var ErrUnsupportedUploadService = errors.New("unsupported compressed log spool upload service")

// UnsupportedUploadServiceErrorは、圧縮ログspoolの再送部品がない保存先サービスを表します。
// errors.Is(err, ErrUnsupportedUploadService)で判定できます。
type UnsupportedUploadServiceError struct {
	// Serviceは、指定された保存先サービスです。
	Service objectstorage.UploadTarget
}

func (e *UnsupportedUploadServiceError) Error() string {
	return fmt.Sprintf("%v %q", ErrUnsupportedUploadService, e.Service)
}

func (e *UnsupportedUploadServiceError) Unwrap() error { return ErrUnsupportedUploadService }

// Newはserviceに対応する送信・分類を選びます。未対応サービスをS3として扱いません。
func New(service objectstorage.UploadTarget, next pkguploader.DestinationUploader) (worker.Uploader, error) {
	switch service {
	case objectstorage.UploadTargetS3:
		upload, err := s3.New(next)
		if err != nil {
			return nil, err
		}
		return upload, nil
	default:
		return nil, &UnsupportedUploadServiceError{Service: service}
	}
}
