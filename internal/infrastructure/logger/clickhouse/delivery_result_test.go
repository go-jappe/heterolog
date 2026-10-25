package clickhouse

import (
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/assert"
)

func TestClickHouseDeliveryFailedResultPreservesError(t *testing.T) {
	wantErr := errors.New("insert failed")
	result := clickHouseDeliveryFailedResult(wantErr)

	assert.Equal(t, deliveryBackendClickHouse, result.Backend)
	assert.Equal(t, logger.DeliveryFailed, result.Status)
	assert.ErrorIs(t, result.Err, wantErr)
}
