package opensearch

import (
	"encoding/json"
	"testing"
	"time"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	loggerfield "github.com/go-jappe/heterolog/internal/infrastructure/logger/field"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type jsonFieldsOtelResource struct{}

func (jsonFieldsOtelResource) ServiceName() string    { return "test-service" }
func (jsonFieldsOtelResource) ServiceVersion() string { return "v1" }
func (jsonFieldsOtelResource) CloudProvider() string  { return "local" }
func (jsonFieldsOtelResource) HostName() string       { return "localhost" }
func (jsonFieldsOtelResource) OSType() string         { return "linux" }
func (jsonFieldsOtelResource) Environment() string    { return "test" }

func TestOpenSearchOtelDocumentAndDurablePayloadPreserveValidatedApplicationFields(t *testing.T) {
	applicationField, err := pkglogger.Any("valid", map[string]any{"attempt": 2})
	require.NoError(t, err)
	_, fields := loggerfield.ExtractIDsAndExtraFields([]pkglogger.Field{applicationField})

	doc := NewOtelConverter(jsonFieldsOtelResource{}).convertLogEntryToDocument(&logging.LogEntry{
		OccurredAt: time.Unix(1, 0).UTC(),
		Message:    "preserve this log",
		Fields:     fields,
	})

	attributes, ok := doc["attributes"].(map[string]any)
	require.True(t, ok)
	documentFields, ok := attributes["fields"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, map[string]any{"attempt": json.Number("2")}, documentFields["valid"])

	request := pkgopensearch.DocumentWriteRequest{
		Index: "logs",
		ID:    "record-id",
		Doc:   doc,
		Op:    pkgopensearch.WriteOpCreate,
	}
	failed := (&OpenSearchFlushManager{}).newFailedDocumentRecord(
		time.Unix(1, 0).UTC(),
		request,
		"test",
		500,
		loggerfailure.FailureCode("test_failure"),
		"test failure",
	)

	_, err = json.Marshal(failed.Payload)
	require.NoError(t, err)
}
