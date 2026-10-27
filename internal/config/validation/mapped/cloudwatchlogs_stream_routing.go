package mappedvalidation

import (
	"errors"
	"time"

	infracloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// ValidateCloudWatchLogsStreamRoutingForLoggerは、同期・非同期Loggerの設定に含まれる
// 通常出力用と内部ログ用のCloudWatch Logs stream名を検証します。基準名に時間区分の
// suffixを付けた後で名前の制約に違反する場合や、時間区分が未対応の場合にエラーを返します。
// nilのCloudWatch Logs設定は検査せず、両方nilなら時間区分も検査しません。
// clientの接続先・認証・log groupや、ローカルのアップロード失敗保存先は対象外です。
func ValidateCloudWatchLogsStreamRoutingForLogger(
	sourceLabel string,
	cfg pkglogger.LoggerConfig,
	granularity pkgcloudwatchlogs.StreamRouting,
) error {
	routes := make([]cloudWatchLogsStreamRoute, 0, 2)
	if cfg.CloudWatchLogs != nil {
		routes = append(routes, cloudWatchLogsStreamRoute{
			sourceLabel:    sourceLabel,
			baseStreamName: cfg.CloudWatchLogs.LogStreamName,
		})
	}
	if destinations := cfg.InternalLogDestinations; destinations != nil &&
		destinations.CloudWatchLogs != nil {
		routes = append(routes, cloudWatchLogsStreamRoute{
			sourceLabel:    sourceLabel + " internal log destination",
			baseStreamName: destinations.CloudWatchLogs.LogStreamName,
		})
	}
	return validateMappedCloudWatchLogsStreamRoutes(granularity, routes)
}

// ValidateCloudWatchLogsStreamRoutingForDeliveryLoggerは、Delivery Loggerの通常出力用と
// 内部ログ用のCloudWatch Logs設定について、時間区分の対応可否とsuffix付加後のstream名を
// 検証します。nilの設定は対象外で、両方nilなら時間区分が不正でもエラーにしません。
// clientの接続先・認証・log groupや、ローカルのアップロード失敗保存先は検査しません。
func ValidateCloudWatchLogsStreamRoutingForDeliveryLogger(
	sourceLabel string,
	cfg pkglogger.DeliveryLoggerConfig,
	granularity pkgcloudwatchlogs.StreamRouting,
) error {
	routes := make([]cloudWatchLogsStreamRoute, 0, 2)
	if cfg.CloudWatchLogs != nil {
		routes = append(routes, cloudWatchLogsStreamRoute{
			sourceLabel:    sourceLabel,
			baseStreamName: cfg.CloudWatchLogs.LogStreamName,
		})
	}
	if destinations := cfg.InternalLogDestinations; destinations != nil &&
		destinations.CloudWatchLogs != nil {
		routes = append(routes, cloudWatchLogsStreamRoute{
			sourceLabel:    sourceLabel + " internal log destination",
			baseStreamName: destinations.CloudWatchLogs.LogStreamName,
		})
	}
	return validateMappedCloudWatchLogsStreamRoutes(granularity, routes)
}

// cloudWatchLogsStreamRouteは、検証する基準stream名とエラーに表示する設定元の組です。
type cloudWatchLogsStreamRoute struct {
	sourceLabel    string
	baseStreamName string
}

// validateMappedCloudWatchLogsStreamRoutesは、noneなら基準名をそのまま、monthly・daily・
// hourlyなら基準名にそれぞれ /YYYY/MM・/YYYY/MM/DD・/YYYY/MM/DD/HH を付けて検査します。
// 時間区分の空値はnoneとして扱い、表記の大小文字や前後の空白は参照先で揃えます。
// 固定日時はsuffixの形式と長さの確認用であり、実際の送信日時やtimezoneは決定しません。
// 組立後の名前が空、512文字超、':'・'*'を含む場合に加え、不正UTF-8、制御文字、不可視の
// 書式制御文字、ASCII space以外の空白、先頭・末尾の空白も参照先のvalidatorで拒否します。
// 不正な名前は設定元ごとにまとめて返します。複数設定が同じstream名でも衝突とは扱いません。
func validateMappedCloudWatchLogsStreamRoutes(
	granularity pkgcloudwatchlogs.StreamRouting,
	routes []cloudWatchLogsStreamRoute,
) error {
	if len(routes) == 0 {
		return nil
	}

	suffix, err := streamrouting.Suffix(
		granularity,
		time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC),
	)
	if err != nil {
		return &ConfigError{
			Source:  routes[0].sourceLabel,
			Section: ConfigSectionCloudWatchLogsStreamRouting,
			Err:     err,
		}
	}

	var errs []error
	for _, route := range routes {
		resolvedStreamName := route.baseStreamName
		if suffix != "" {
			resolvedStreamName += "/" + suffix
		}
		if err := infracloudwatchlogs.ValidateApplicationLogStreamName(resolvedStreamName); err != nil {
			errs = append(errs, &CloudWatchLogsStreamNameError{
				Source:     route.sourceLabel,
				StreamName: resolvedStreamName,
				Err:        err,
			})
		}
	}
	return errors.Join(errs...)
}
