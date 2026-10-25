package opensearch

import (
	"errors"
	"fmt"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/wrapper"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

// NewClientが失敗した段階を表すerrorの種類です。原因のerrorはerrors.Is・errors.Asで辿れます。
var (
	// ErrNilClientConfigは、OpenSearch clientの設定providerがnilであることを表します。
	ErrNilClientConfig = errors.New("opensearch client config is nil")
	// ErrConvertClientConfigは、設定providerからOpenSearch clientの設定を作成できなかったことを表します。
	ErrConvertClientConfig = errors.New("convert opensearch client config")
	// ErrCreateClientは、設定からOpenSearch clientを作成できなかったことを表します。
	ErrCreateClient = errors.New("create opensearch client")
	// ErrBuildClientChainは、retry wrapperとcircuit breakerを組み立てられなかったことを表します。
	ErrBuildClientChain = errors.New("build opensearch client chain")
)

// NewClientは設定からOpenSearch clientを作成し、設定されたtransport retryと
// circuit breakerを追加します。固定ID createの409は呼び出し後のbackend結果分類で扱います。
func NewClient(
	clientCfg OpenSearchClientConfigProvider,
	retryCfg wrapper.RetryConfigProvider,
	tracer observer.Tracer,
	meter observer.Meter,
) (pkgopensearch.Client, error) {
	if clientCfg == nil {
		return nil, ErrNilClientConfig
	}

	osConfig, err := ToClientConfig(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConvertClientConfig, err)
	}

	client, err := pkgopensearch.New(osConfig)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCreateClient, err)
	}

	wrapped, err := wrapper.BuildClientChain(client, retryCfg, tracer, meter)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBuildClientChain, err)
	}
	return wrapped, nil
}
