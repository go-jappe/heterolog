package multi

import infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"

// unknownBackendNameは、名前を返さないchildのmetricsのbackend属性の値です。
const unknownBackendName = "unknown"

// backendNamerは、metricsのbackend属性に使う出力先の名前を返すchildです。
type backendNamer interface {
	BackendName() string
}

// backendNamesは、childごとのmetricsのbackend属性の値を、childと同じ順で返します。
func backendNames(loggers []infralogging.BackendLogEntryHandler) []string {
	names := make([]string, len(loggers))
	for index, l := range loggers {
		names[index] = unknownBackendName
		if namer, ok := l.(backendNamer); ok {
			names[index] = namer.BackendName()
		}
	}
	return names
}
