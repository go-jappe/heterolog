package clickhouse

import (
	"testing"
	"time"

	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClientReturnsRawClientWhenRetryConfigIsNil(t *testing.T) {
	t.Parallel()

	client, err := NewClient(newTestClientConfig(), nil, nil, nil)

	require.NoError(t, err)
	assert.IsType(t, &pkgclickhouse.Client{}, client)
}

func TestNewClientReturnsErrorForInvalidConfig(t *testing.T) {
	t.Parallel()

	client, err := NewClient(pkgclickhouse.Config{}, nil, nil, nil)

	require.ErrorIs(t, err, ErrCreateClient)
	assert.Nil(t, client)
}

func newTestClientConfig() pkgclickhouse.Config {
	return pkgclickhouse.Config{
		Endpoint: "http://localhost:8123",
		Table:    "logs",
		Timeout:  time.Second,
	}
}
