package cloudwatchlogs

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/wrapper"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

// NewClientが失敗した段階を表すerrorの種類です。原因のerrorはerrors.Is・errors.Asで辿れます。
var (
	// ErrCreateClientは、設定からCloudWatch Logs clientを作成できなかったことを表します。
	ErrCreateClient = errors.New("create cloudwatch logs client")
	// ErrBuildClientChainは、retry wrapperとcircuit breakerを組み立てられなかったことを表します。
	ErrBuildClientChain = errors.New("build cloudwatch logs client chain")
)

// NewClientはCloudWatch Logsのraw HTTP clientを作成し、設定されたretry／circuit breakerを
// 確定済みrequestの送信操作へ適用します。log streamのroutingは扱いません。
func NewClient(
	cfg pkgcloudwatchlogs.Config,
	retryCfg wrapper.RetryConfigProvider,
	tracer observer.Tracer,
	meter observer.Meter,
) (pkgcloudwatchlogs.Client, error) {
	client, err := pkgcloudwatchlogs.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCreateClient, err)
	}

	wrapped, err := wrapper.BuildClientChain(client, retryCfg, tracer, meter)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBuildClientChain, err)
	}
	return wrapped, nil
}
