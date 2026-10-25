package cloudwatchlogs

import (
	"encoding/json"
	"testing"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	loggerfield "github.com/go-jappe/heterolog/internal/infrastructure/logger/field"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudWatchLogsDurableDocumentPreservesValidatedApplicationFields(t *testing.T) {
	applicationField, err := applogging.Any("valid", []any{"attempt", 2})
	require.NoError(t, err)
	_, fields := loggerfield.ExtractIDsAndExtraFields([]applogging.Field{applicationField})

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
