package cloudwatchlogs

import (
	"testing"

	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClientReturnsRawHTTPClientWhenRetryConfigIsNil(t *testing.T) {
	t.Parallel()

	client, err := NewClient(newTestClientConfig(), nil, nil, nil)

	require.NoError(t, err)
	assert.IsType(t, &pkgcloudwatchlogs.HTTPClient{}, client)
}

func TestNewClientReturnsErrorForInvalidConfig(t *testing.T) {
	t.Parallel()

	client, err := NewClient(pkgcloudwatchlogs.Config{}, nil, nil, nil)

	require.ErrorIs(t, err, ErrCreateClient)
	assert.Nil(t, client)
	assert.ErrorIs(t, err, pkgcloudwatchlogs.ErrLogGroupNameRequired)
}

func newTestClientConfig() pkgcloudwatchlogs.Config {
	return pkgcloudwatchlogs.Config{
		Region:          "ap-northeast-1",
		Endpoint:        "http://localhost:4566",
		LogGroupName:    "/app/test",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
	}
}
