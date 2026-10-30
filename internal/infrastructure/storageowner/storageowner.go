// Package storageownerは、object storageへ送れなかった圧縮ログを保存するdirectory
// （圧縮ログのspoolとfailure archive）を、所有者ごとに分ける名前を定義します。名前は
// directoryのpathの要素になり、所有者ごとに別のdirectoryを使います。
package storageowner

import pkglogger "github.com/go-jappe/heterolog/pkg/logger"

// Telemetryは、NewTelemetryのtraceのfailure archiveが所有する保存先の名前です。loggerの
// 契約ではありませんが、loggerの契約と同じ位置に使い、loggerの保存先と分けます。同じprocessで
// loggerとTelemetryが同じ保存先を使うと、所有権の予約がぶつかるためです。
const Telemetry pkglogger.LoggerContractKind = "telemetry"

// Supportedは、kindが、圧縮ログの保存先を所有できる名前かを返します。所有者は、loggerの
// 契約（pkg/logger.LoggerContractKind）とTelemetryです。
func Supported(kind pkglogger.LoggerContractKind) bool {
	switch kind {
	case pkglogger.LoggerContractSync,
		pkglogger.LoggerContractAsyncBestEffort,
		pkglogger.LoggerContractDelivery,
		Telemetry:
		return true
	default:
		return false
	}
}
