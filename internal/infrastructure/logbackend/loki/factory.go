package loki

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki/wrapper"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

// NewClientが失敗した段階を表すerrorの種類です。原因のerrorはerrors.Is・errors.Asで辿れます。
var (
	// ErrCreateClientは、設定からLoki clientを作成できなかったことを表します。
	ErrCreateClient = errors.New("create loki client")
	// ErrBuildClientChainは、retry wrapperとcircuit breakerを組み立てられなかったことを表します。
	ErrBuildClientChain = errors.New("build loki client chain")
)

// NewClientは設定からLoki clientを作成し、指定されたretry wrapperを適用します。
func NewClient(
	cfg pkgloki.Config,
	retryCfg wrapper.RetryConfigProvider,
	tracer observer.Tracer,
	meter observer.Meter,
) (wrapper.Client, error) {
	client, err := pkgloki.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCreateClient, err)
	}

	wrapped, err := wrapper.BuildClientChain(client, retryCfg, tracer, meter)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBuildClientChain, err)
	}
	return wrapped, nil
}
