package loki

import (
	"encoding/json"
	"testing"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	loggerfield "github.com/go-jappe/heterolog/internal/infrastructure/logger/field"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLokiDurableDocumentPreservesValidatedApplicationFields(t *testing.T) {
	applicationField, err := applogging.Any("valid", map[string]any{"attempt": 2})
	require.NoError(t, err)
	_, fields := loggerfield.ExtractIDsAndExtraFields([]applogging.Field{applicationField})

	doc := (&LokiFlushManager{}).convertLogEntryRecordToDocument(lokiLogEntryRecord{
		logEntry: &logging.LogEntry{
			Message: "preserve this log",
			Fields:  fields,
		},
		labels: map[string]string{"app": "test"},
		pushEntry: pkgloki.Entry{
			Timestamp: time.Unix(1, 0).UTC(),
			Line:      "formatted line",
		},
	})

	documentFields, ok := doc["fields"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, map[string]any{"attempt": json.Number("2")}, documentFields["valid"])

	_, err = json.Marshal(doc)
	require.NoError(t, err)
}
