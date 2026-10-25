//go:build !windows

package s3

import (
	"context"
	"os"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"

	"github.com/stretchr/testify/require"
)

func TestClientUploadRejectsDeviceBeforeSending(t *testing.T) {
	client, requests := newFileTypeTestClient(t)
	err := client.Upload(context.Background(), uploadertest.Input(t, os.DevNull, "logs/device"))
	require.ErrorIs(t, err, ErrNotRegularFile)
	require.Zero(t, *requests)
}
