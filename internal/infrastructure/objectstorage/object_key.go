package objectstorage

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// ObjectKeyUploaderは、この宛先のprefixと送信元のファイル名から保存名を作り、
// 保存名が確定したUploadInputを送信実装へ渡します。
type ObjectKeyUploader struct {
	next   objectUploader
	prefix string
}

// objectUploaderは、保存名を確定した入力を受け取る送信実装に必要な操作です。
type objectUploader interface {
	Upload(context.Context, uploader.UploadInput) error
}

func NewObjectKeyUploader(next objectUploader, prefix string) uploader.Uploader {
	if next == nil {
		return nil
	}
	return &ObjectKeyUploader{next: next, prefix: prefix}
}

func (u *ObjectKeyUploader) Upload(ctx context.Context, request uploader.UploadRequest) error {
	input, err := uploader.NewUploadInput(request, BuildObjectKey(u.prefix, request.FilePath()))
	if err != nil {
		return err
	}
	return u.next.Upload(ctx, input)
}

func BuildObjectKey(prefix string, filePath string) string {
	fileName := filepath.Base(filePath)
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return fileName
	}
	return prefix + "/" + fileName
}
