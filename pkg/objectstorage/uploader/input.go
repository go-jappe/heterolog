package uploader

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// ErrInvalidUploadRequestは、UploadRequestがNewUploadRequestで構築されておらず、
// 送信元の絶対パスを持たないことを表します。
var ErrInvalidUploadRequest = errors.New("upload request requires an absolute file path")

// UploadRequestは、宛先ごとの保存名を決める前の送信依頼です。
// NewUploadRequestで送信元を絶対パスに確定します。ゼロ値は使用できません。
type UploadRequest struct {
	filePath string
}

// NewUploadRequestは空のパスを拒否し、相対パスを現在の作業ディレクトリから
// 絶対パスへ変換します。大小文字を変更せず、ファイルの存在やリンクは調べません。
func NewUploadRequest(filePath string) (UploadRequest, error) {
	if strings.TrimSpace(filePath) == "" {
		return UploadRequest{}, errors.New("upload file path is required")
	}
	absolute, err := filepath.Abs(filePath)
	if err != nil {
		return UploadRequest{}, fmt.Errorf("resolve upload file path %q: %w", filePath, err)
	}
	return UploadRequest{filePath: absolute}, nil
}

// FilePathは構築時に確定した送信元の絶対パスを返します。
func (r UploadRequest) FilePath() string { return r.filePath }

// Validateは送信入口でゼロ値などの未構築の依頼を、ErrInvalidUploadRequestとして拒否します。
func (r UploadRequest) Validate() error {
	if !filepath.IsAbs(r.filePath) {
		return ErrInvalidUploadRequest
	}
	return nil
}

// UploadInputは、送信元の絶対パスと宛先内の保存名が確定した入力です。
// NewUploadInputで構築します。ゼロ値は使用できません。
type UploadInput struct {
	request   UploadRequest
	objectKey string
}

// NewUploadInputは依頼の絶対パスを引き継ぎ、空でない保存名を設定します。
// objectKeyには宛先のprefixを含む完成済みのキーを渡します。前後の空白も
// 保存名の一部として保持し、空文字だけを拒否します。
func NewUploadInput(request UploadRequest, objectKey string) (UploadInput, error) {
	if err := request.Validate(); err != nil {
		return UploadInput{}, err
	}
	if objectKey == "" {
		return UploadInput{}, errors.New("object storage object key is required")
	}
	return UploadInput{request: request, objectKey: objectKey}, nil
}

// FilePathは送信元の絶対パスを返します。
func (i UploadInput) FilePath() string { return i.request.FilePath() }

// ObjectKeyは宛先のprefixを含む完成済みの保存名を返します。
func (i UploadInput) ObjectKey() string { return i.objectKey }

// Validateは送信入口で未構築の入力を拒否します。
func (i UploadInput) Validate() error {
	if err := i.request.Validate(); err != nil {
		return err
	}
	if i.objectKey == "" {
		return errors.New("object storage object key is required")
	}
	return nil
}
