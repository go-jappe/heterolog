package wrapper

// backendNameは、metricsのbackend属性の値です。
const backendName = "opensearch"

const (
	spanNameIndexDocument = "heterolog.opensearch.index.attempt"
	spanNameBulk          = "heterolog.opensearch.bulk.attempt"
)
