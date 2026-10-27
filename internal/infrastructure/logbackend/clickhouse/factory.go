package clickhouse

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse/wrapper"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// NewClientが失敗した段階を表すerrorの種類です。原因のerrorはerrors.Is・errors.Asで辿れます。
var (
	// ErrCreateClientは、設定からClickHouse clientを作成できなかったことを表します。
	ErrCreateClient = errors.New("create clickhouse client")
	// ErrBuildClientChainは、retry wrapperとcircuit breakerを組み立てられなかったことを表します。
	ErrBuildClientChain = errors.New("build clickhouse client chain")
)

// NewClientは設定からClickHouse clientを作成し、指定されたretry wrapperを適用します。
func NewClient(
	cfg pkgclickhouse.Config,
	retryCfg wrapper.RetryConfigProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (wrapper.Client, error) {
	client, err := pkgclickhouse.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCreateClient, err)
	}

	wrapped, err := wrapper.BuildClientChain(client, retryCfg, tracer, meter)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBuildClientChain, err)
	}
	return wrapped, nil
}
