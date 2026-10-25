package opensearch

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logentry"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
	opensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

// アプリケーションから渡されたentryをopensearch用のentryに変換
// ※Otelではないので注意
func ConvertToIndexableLogEntry(
	e logging.LogEntry,
	timestampFormatter timestamp.Formatter,
) logentry.IndexableLogEntry {
	var errMsg string
	if e.Err != nil {
		errMsg = e.Err.Error()
	}
	return logentry.IndexableLogEntry{
		Timestamp:     timestampFormatter.Format(logging.ResolveEntryOccurredAt(&e)),
		Level:         e.Level.String(),
		Message:       e.Message,
		RequestId:     e.RequestID,
		File:          e.File,
		Line:          e.Line,
		FuncName:      e.FuncName,
		Err:           errMsg,
		InternalError: e.Internal,
		Source:        e.Source,
		Fields:        e.Fields,
	}
}

// OpenSearchClientConfigProviderからopensearch.Configへの変換
func ToClientConfig(cfg OpenSearchClientConfigProvider) (opensearch.Config, error) {
	authConfig, err := cfg.AuthConfig()
	if err != nil {
		return opensearch.Config{}, err
	}

	var authCfg opensearch.AuthConfig
	if authConfig != nil {
		authCfg = authConfig.ToAuthConfig()
	} else {
		// 認証なしの場合
		authCfg = opensearch.AuthConfig{
			Type: opensearch.AuthTypeNone,
		}
	}

	osConfig := opensearch.Config{
		Endpoint:        cfg.Endpoint(),
		Timeout:         cfg.Timeout(),
		SkipVerifySSL:   cfg.SkipVerifySSL(),
		CompressRequest: cfg.CompressRequest(),
		AuthConfig:      authCfg,
	}
	return osConfig, nil
}
