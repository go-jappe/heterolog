package evacuation

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"

	"github.com/stretchr/testify/require"
)

func TestWriteTextPreservesFormattedContent(t *testing.T) {
	var output bytes.Buffer
	text := "2026-09-27 INFO original log\nsecond line\n"
	require.NoError(t, writeText(context.Background(), &output, text))
	require.Equal(t, text, output.String())
}

type textTestWriter func([]byte) (int, error)

func (w textTestWriter) Write(data []byte) (int, error) { return w(data) }

func TestWriteTextReturnsWriteFailures(t *testing.T) {
	diskError := errors.New("write failed")
	for _, test := range []struct {
		name   string
		writer io.Writer
		want   error
	}{
		{"short write", textTestWriter(func(data []byte) (int, error) { return len(data) - 1, nil }), io.ErrShortWrite},
		{"write error", textTestWriter(func([]byte) (int, error) { return 0, diskError }), diskError},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.ErrorIs(t, writeText(context.Background(), test.writer, "original\n"), test.want)
		})
	}
}

func TestStdoutEvacuationLockWaitCanBeCanceled(t *testing.T) {
	for _, kind := range []string{"text", "records"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var output bytes.Buffer
				stdoutBatchMu.Lock()
				defer stdoutBatchMu.Unlock()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					if kind == "text" {
						done <- writeText(ctx, &output, "original\n")
					} else {
						done <- newStdoutHandler(&output).Handle(ctx, loggerfailure.SendResult{})
					}
				}()
				synctest.Wait()
				cancel()
				require.ErrorIs(t, <-done, context.Canceled)
				require.Empty(t, output.String())
			})
		})
	}
}

func TestWriteTextDoesNotInterleaveWithFailureBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writer := newBlockingMetaWriter(t)
		done := make(chan error, 2)
		go func() {
			done <- newStdoutHandler(writer).Handle(context.Background(), loggerfailure.SendResult{
				Meta:             loggerfailure.Meta{RequestID: "request-a"},
				RetryableRecords: []loggerfailure.FailedRecord{{Payload: map[string]any{"message": "failure"}}},
			})
		}()
		<-writer.firstMetaWritten
		textWritten := make(chan struct{})
		go func() {
			done <- writeText(context.Background(), writer, "original file log\n")
			close(textWritten)
		}()
		synctest.Wait()
		select {
		case <-textWritten:
			t.Fatal("file log was written inside a failure batch")
		default:
		}
		writer.releaseFirst()
		require.NoError(t, <-done)
		require.NoError(t, <-done)
		synctest.Wait()
		require.Contains(t, string(writer.Bytes()), "\"payload\":{\"message\":\"failure\"}")
		require.Contains(t, string(writer.Bytes()), "\noriginal file log\n")
	})
}
