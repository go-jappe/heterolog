package cloudwatchlogs

import (
	"encoding/json"
	"testing"
	"time"

	loggerfield "github.com/go-jappe/heterolog/internal/infrastructure/logger/field"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudWatchLogsDurableDocumentPreservesValidatedApplicationFields(t *testing.T) {
	applicationField, err := pkglogger.Any("valid", []any{"attempt", 2})
	require.NoError(t, err)
	_, fields := loggerfield.ExtractIDsAndExtraFields([]pkglogger.Field{applicationField})

	doc := (&CloudWatchLogsFlushManager{}).convertEventRecordToDocument(
		cloudWatchLogsEventRecord{
			entry: &logging.LogEntry{
				Message: "preserve this log",
				Fields:  fields,
			},
			event: pkgcloudwatchlogs.Event{
				Timestamp: time.Unix(1, 0).UTC(),
				Message:   "formatted line",
			},
		},
		"application-20260815",
	)

	documentFields, ok := doc["fields"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, []any{"attempt", json.Number("2")}, documentFields["valid"])

	_, err = json.Marshal(doc)
	require.NoError(t, err)
}
