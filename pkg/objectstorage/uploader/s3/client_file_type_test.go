package s3

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"

	"github.com/stretchr/testify/require"
)

func TestClientUploadRejectsNonRegularFileBeforeSending(t *testing.T) {
	for _, kind := range []string{"file-link", "dangling-link", "directory-link", "loop-link", "directory"} {
		t.Run(kind, func(t *testing.T) {
			inputPath := filepath.Join(t.TempDir(), "rotated.log.gz")
			targetPath := filepath.Join(t.TempDir(), "target")
			const targetContent = "must not be uploaded through a link"
			switch kind {
			case "file-link":
				require.NoError(t, os.WriteFile(targetPath, []byte(targetContent), 0o600))
				testutil.Symlink(t, targetPath, inputPath)
			case "dangling-link":
				testutil.Symlink(t, targetPath, inputPath)
			case "directory-link":
				require.NoError(t, os.Mkdir(targetPath, 0o700))
				testutil.Symlink(t, targetPath, inputPath)
			case "loop-link":
				targetPath = inputPath
				testutil.Symlink(t, targetPath, inputPath)
			case "directory":
				require.NoError(t, os.Mkdir(inputPath, 0o700))
			}

			client, requests := newFileTypeTestClient(t)
			err := client.Upload(context.Background(), uploadertest.Input(t, inputPath, "logs/rotated.log.gz"))
			require.ErrorIs(t, err, ErrNotRegularFile)
			require.Zero(t, *requests, "unsafe input must not reach the HTTP transport")
			info, err := os.Lstat(inputPath)
			require.NoError(t, err)
			if kind == "directory" {
				require.True(t, info.IsDir())
			} else {
				require.NotZero(t, info.Mode()&os.ModeSymlink)
				gotTarget, err := os.Readlink(inputPath)
				require.NoError(t, err)
				require.Equal(t, targetPath, gotTarget)
			}
			if kind == "file-link" {
				content, err := os.ReadFile(targetPath)
				require.NoError(t, err)
				require.Equal(t, targetContent, string(content))
			}
			if kind == "dangling-link" {
				_, err := os.Lstat(targetPath)
				require.ErrorIs(t, err, os.ErrNotExist)
			}
		})
	}
}

func TestClientUploadPreservesMissingFileErrorWithoutSending(t *testing.T) {
	client, requests := newFileTypeTestClient(t)
	err := client.Upload(context.Background(), uploadertest.Input(t,
		filepath.Join(t.TempDir(), "missing.log.gz"), "logs/missing.log.gz"))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Zero(t, *requests)
}

func TestClientUploadAcceptsEmptyRegularFile(t *testing.T) {
	inputPath := filepath.Join(t.TempDir(), "empty.log.gz")
	require.NoError(t, os.WriteFile(inputPath, nil, 0o600))
	client, requests := newFileTypeTestClient(t)
	require.NoError(t, client.Upload(context.Background(), uploadertest.Input(t, inputPath, "logs/empty.log.gz")))
	require.Equal(t, 1, *requests)
}

func TestClientUploadRejectsZeroInputBeforeSending(t *testing.T) {
	client, requests := newFileTypeTestClient(t)
	err := client.Upload(context.Background(), uploader.UploadInput{})
	require.ErrorIs(t, err, uploader.ErrInvalidUploadRequest)
	require.Zero(t, *requests)
}

func newFileTypeTestClient(t *testing.T) (*Client, *int) {
	t.Helper()
	requests := 0
	client, err := New(Config{
		S3Config: objectstorage.S3Config{
			Region: "ap-northeast-1", Endpoint: "http://s3.invalid", Bucket: "archive-bucket",
			AccessKeyID: "test-access-key", SecretAccessKey: "test-secret-key",
		},
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			_, err := io.Copy(io.Discard, request.Body)
			require.NoError(t, err)
			return &http.Response{
				StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")),
				Header: make(http.Header), Request: request,
			}, nil
		})},
	})
	require.NoError(t, err)
	return client, &requests
}
