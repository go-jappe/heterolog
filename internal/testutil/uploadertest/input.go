// Package uploadertestは試験用の送信入力をコンストラクタ経由で作成します。
package uploadertest

import (
	"testing"

	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func Request(t testing.TB, path string) uploader.UploadRequest {
	t.Helper()
	request, err := uploader.NewUploadRequest(path)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func Input(t testing.TB, path, key string) uploader.UploadInput {
	t.Helper()
	input, err := uploader.NewUploadInput(Request(t, path), key)
	if err != nil {
		t.Fatal(err)
	}
	return input
}
