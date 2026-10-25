package config

import pkglogger "github.com/go-jappe/heterolog/pkg/logger"

// loggerOutputSourceはpkg/loggerで一元管理するLoggerContractKindのcanonical IDと型付き
// output destinationから、`<contract>.<destination>`形式の診断用source名を生成します。
func loggerOutputSource(
	contractKind pkglogger.LoggerContractKind,
	destination pkglogger.LogOutputDestination,
) string {
	return contractKind.String() + "." + destination.String()
}

// loggerInternalLogOutputSourceは通常出力先と異なるinternal-log専用destinationの
// 診断用source名を生成します。
func loggerInternalLogOutputSource(
	contractKind pkglogger.LoggerContractKind,
	destination pkglogger.LogOutputDestination,
) string {
	return contractKind.String() + ".internal_log." + destination.String()
}
