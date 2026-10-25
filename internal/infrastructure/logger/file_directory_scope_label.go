package logger

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

const (
	fileDirectoryScopeLabelSuffix            = ".file"
	internalLogFileDirectoryScopeLabelSuffix = ".internal_log.file"

	// 各値はLoggerContractKindとfileの用途から組み立てるdirectory Leaseの診断用
	// ScopeLabelです。LoggerContractKindそのものや個々のLogger instance IDではありません。
	syncFileDirectoryScopeLabel directorylease.ScopeLabel = directorylease.ScopeLabel(
		string(pkglogger.LoggerContractSync) + fileDirectoryScopeLabelSuffix,
	)
	asyncFileDirectoryScopeLabel directorylease.ScopeLabel = directorylease.ScopeLabel(
		string(pkglogger.LoggerContractAsyncBestEffort) + fileDirectoryScopeLabelSuffix,
	)
	deliveryFileDirectoryScopeLabel directorylease.ScopeLabel = directorylease.ScopeLabel(
		string(pkglogger.LoggerContractDelivery) + fileDirectoryScopeLabelSuffix,
	)
	syncInternalLogFileDirectoryScopeLabel directorylease.ScopeLabel = directorylease.ScopeLabel(
		string(pkglogger.LoggerContractSync) + internalLogFileDirectoryScopeLabelSuffix,
	)
	asyncInternalLogFileDirectoryScopeLabel directorylease.ScopeLabel = directorylease.ScopeLabel(
		string(pkglogger.LoggerContractAsyncBestEffort) + internalLogFileDirectoryScopeLabelSuffix,
	)
	deliveryInternalLogFileDirectoryScopeLabel directorylease.ScopeLabel = directorylease.ScopeLabel(
		string(pkglogger.LoggerContractDelivery) + internalLogFileDirectoryScopeLabelSuffix,
	)
)
