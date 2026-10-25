package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

func TestSpoolPayloadRejectsDataAfterCompleteJSON(t *testing.T) {
	for _, suffix := range []string{"{}", "null", "garbage", "["} {
		t.Run(suffix, func(t *testing.T) {
			client := &recordingClickHouseSpoolClient{}
			sender := newTestClickHouseSpoolSender(t, time.Now(), client)
			record := clickHouseSpoolRecord(t, "one", "payload", "token")
			record.Payload = append(record.Payload, suffix...)
			report, err := sender.Send(context.Background(), []spool.SpoolRecord{record})
			require.NoError(t, err)
			require.NoError(t, report.Validate(1))
			require.Equal(t, spool.RecordOutcomeStatusInvalid, report[0].Status)
			require.ErrorIs(t, report[0].Err, ErrDecodeSpoolPayload)
			require.Zero(t, client.calls)
		})
	}
}
