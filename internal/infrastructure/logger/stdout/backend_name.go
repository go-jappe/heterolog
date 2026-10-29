package stdout

import pkglogger "github.com/go-jappe/heterolog/pkg/logger"

// BackendNameは、metricsのbackend属性に使う出力先の名前を返します。
func (*StdoutLogger) BackendName() string { return string(pkglogger.LogOutputStdout) }
