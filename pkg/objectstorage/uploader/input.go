package uploader

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// ErrInvalidUploadRequest reports that an UploadRequest was not built with
// NewUploadRequest and has no absolute source path.
var ErrInvalidUploadRequest = errors.New("upload request requires an absolute file path")

// UploadRequest is an upload request before the object name for each
// destination is decided. NewUploadRequest fixes the source as an absolute
// path. The zero value cannot be used.
type UploadRequest struct {
	filePath string
}

// NewUploadRequest rejects an empty or blank path and converts the path with
// filepath.Abs: a relative path is made absolute from the current working
// directory, and the result is cleaned. It does not change letter case, and it
// does not check whether the file exists or inspect links.
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

// FilePath returns the absolute source path fixed at construction.
func (r UploadRequest) FilePath() string { return r.filePath }

// Validate rejects, at the upload entry point, a request that was not
// constructed, such as the zero value, with ErrInvalidUploadRequest.
func (r UploadRequest) Validate() error {
	if !filepath.IsAbs(r.filePath) {
		return ErrInvalidUploadRequest
	}
	return nil
}

// UploadInput is an upload input whose absolute source path and object name in
// the destination are fixed. Build one with NewUploadInput. The zero value
// cannot be used.
type UploadInput struct {
	request   UploadRequest
	objectKey string
}

// NewUploadInput carries over the request's absolute path and sets a non-empty
// object name. Pass the complete key, including the destination's prefix, as
// objectKey. Leading and trailing whitespace is kept as part of the object
// name, and only the empty string is rejected.
func NewUploadInput(request UploadRequest, objectKey string) (UploadInput, error) {
	if err := request.Validate(); err != nil {
		return UploadInput{}, err
	}
	if objectKey == "" {
		return UploadInput{}, errors.New("object storage object key is required")
	}
	return UploadInput{request: request, objectKey: objectKey}, nil
}

// FilePath returns the absolute source path.
func (i UploadInput) FilePath() string { return i.request.FilePath() }

// ObjectKey returns the complete object name, including the destination's
// prefix.
func (i UploadInput) ObjectKey() string { return i.objectKey }

// Validate rejects, at the upload entry point, an input that was not
// constructed.
func (i UploadInput) Validate() error {
	if err := i.request.Validate(); err != nil {
		return err
	}
	if i.objectKey == "" {
		return errors.New("object storage object key is required")
	}
	return nil
}
