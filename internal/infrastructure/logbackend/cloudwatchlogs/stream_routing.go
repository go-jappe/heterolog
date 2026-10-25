package cloudwatchlogs

import (
	"errors"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
)

// ErrEventTimeRequiredは、stream名の解決にevent発生日時が必要なのにzero値だったことを表します。
var ErrEventTimeRequired = errors.New("cloudwatch logs event time is required to resolve log stream")

// LogStreamNameResolverは、event発生日時から初回送信で実際に使用するCloudWatch Logsの
// stream名を解決します。request plannerがrouting時に使用し、確定したstream名は失敗時の
// durable payloadへ保存します。spool再送では設定から再計算せず、その保存値を使用します。
// runtimeで実装を差し替える契約はなく、CloudWatch Logsのrequest計画が使用する具象型です。
type LogStreamNameResolver struct {
	baseName           string
	routingGranularity streamrouting.Granularity
}

// NewLogStreamNameResolverは、基準stream名とrouting granularityから、
// event発生日時だけを入力とするrouting依存を作ります。このresolverは現在時刻を取得せず、
// CloudWatch Logsへの通信やretry状態も持ちません。
func NewLogStreamNameResolver(
	baseName string,
	routingGranularity streamrouting.Granularity,
) (*LogStreamNameResolver, error) {
	routingGranularity = streamrouting.Normalize(routingGranularity)
	if _, err := ResolveLogStreamName(
		baseName,
		routingGranularity,
		time.Date(2006, time.January, 2, 15, 0, 0, 0, time.UTC),
	); err != nil {
		return nil, err
	}

	return &LogStreamNameResolver{
		baseName:           baseName,
		routingGranularity: routingGranularity,
	}, nil
}

// ResolveLogStreamNameForEventは、event発生日時のタイムゾーンを変えずに、設定された
// routing granularityに対応するstream名を返します。ログ本文の表示設定は使いません。
func (r *LogStreamNameResolver) ResolveLogStreamNameForEvent(at time.Time) (string, error) {
	if at.IsZero() {
		return "", ErrEventTimeRequired
	}
	return ResolveLogStreamName(r.baseName, r.routingGranularity, at)
}

// ResolveLogStreamNameは、基準stream名へevent発生日時に対応するsuffixを付けます。
// atのLocationをそのまま使用し、生成後の名前はAWS制約とapplicationの運用制約で検証します。
func ResolveLogStreamName(
	baseName string,
	routingGranularity streamrouting.Granularity,
	at time.Time,
) (string, error) {
	suffix, err := streamrouting.Suffix(routingGranularity, at)
	if err != nil {
		return "", err
	}
	if suffix == "" {
		if err := ValidateApplicationLogStreamName(baseName); err != nil {
			return "", err
		}
		return baseName, nil
	}

	resolved := baseName + "/" + suffix
	if err := ValidateApplicationLogStreamName(resolved); err != nil {
		return "", err
	}
	return resolved, nil
}
