package status

import (
	"slices"

	"github.com/go-jappe/heterolog/internal/application/metricnames"
	"github.com/go-jappe/heterolog/internal/application/metrics"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// metricIndexは、名前ごとのmetricsです。
type metricIndex map[string][]metrics.Point

func indexMetrics(collected []metrics.Metric) metricIndex {
	index := metricIndex{}
	for _, metric := range collected {
		index[metric.Name] = append(index[metric.Name], metric.Points...)
	}
	return index
}

// pointsWithは、名前がnameで、属性keyの値がvalueのpointを返します。
func (index metricIndex) pointsWith(name, key, value string) []metrics.Point {
	var result []metrics.Point
	for _, point := range index[name] {
		if attrValue(point.Attrs, key) == value {
			result = append(result, point)
		}
	}
	return result
}

func attrValue(attrs []pkgobserver.Attr, key string) string {
	for _, attr := range attrs {
		if attr.Key == key {
			if s, ok := attr.Value.(string); ok {
				return s
			}
		}
	}
	return ""
}

// sumByは、pointsのInt値を、属性keyの値ごとに合計します。
func sumBy(points []metrics.Point, key string) map[string]int64 {
	sums := map[string]int64{}
	for _, point := range points {
		sums[attrValue(point.Attrs, key)] += point.Int
	}
	return sums
}

func sumAll(points []metrics.Point) int64 {
	var total int64
	for _, point := range points {
		total += point.Int
	}
	return total
}

// backendMetricNamesは、出力先の種類ごとのmetricsの名前です。
type backendMetricNames struct {
	records, duration, retries string
	// keyは、records・duration・retriesで出力先を選ぶ属性です。
	key string
}

func metricNamesOf(signal string) backendMetricNames {
	switch signal {
	case SignalTraces:
		return backendMetricNames{metricnames.TraceSpans, metricnames.TraceExportDuration, metricnames.TraceExportRetries, metricnames.AttrExporter}
	case SignalMetrics:
		return backendMetricNames{metricnames.MetricPoints, metricnames.MetricExportDuration, metricnames.MetricExportRetries, metricnames.AttrExporter}
	default:
		return backendMetricNames{metricnames.BackendRecords, metricnames.BackendRequestDuration, metricnames.BackendRetries, metricnames.AttrBackend}
	}
}

// deriveBackendは、一つの出力先のmetricsを読み、報告にします。spoolとdirectoryの値は
// 含めません（Serviceが足します）。
func deriveBackend(index metricIndex, inspection BackendInspection) Backend {
	names := metricNamesOf(inspection.Signal)
	selector := inspection.Name
	if names.key == metricnames.AttrExporter {
		selector = inspection.Exporter
	}
	backend := Backend{Name: inspection.Name}

	outcomes := sumBy(index.pointsWith(names.records, names.key, selector), metricnames.AttrOutcome)
	backend.Records = Records{
		Success:    outcomes[metricnames.OutcomeSuccess],
		Retryable:  outcomes[metricnames.OutcomeRetryable],
		Dead:       outcomes[metricnames.OutcomeDead],
		Quarantine: outcomes[metricnames.OutcomeQuarantine],
		Failure:    outcomes[metricnames.OutcomeFailure],
		Dropped:    outcomes[metricnames.OutcomeDropped],
	}
	if durations := index.pointsWith(names.duration, names.key, selector); len(durations) > 0 {
		requests := &Requests{}
		var sum float64
		for _, point := range durations {
			requests.Count += int64(point.Count)
			sum += point.Sum
			if point.HasMinMax && point.Max > requests.MaxSeconds {
				requests.MaxSeconds = point.Max
			}
			if attrValue(point.Attrs, metricnames.AttrOutcome) == metricnames.OutcomeFailure {
				requests.Failures += int64(point.Count)
			}
		}
		if requests.Count > 0 {
			requests.AverageSeconds = sum / float64(requests.Count)
		}
		backend.Requests = requests
	}
	backend.Retries = sumAll(index.pointsWith(names.retries, names.key, selector))

	transitions := index.pointsWith(metricnames.CircuitBreakerTransitions, metricnames.AttrBackend, inspection.Name)
	rejected := index.pointsWith(metricnames.CircuitBreakerRejectedRequests, metricnames.AttrBackend, inspection.Name)
	if len(transitions) > 0 || len(rejected) > 0 {
		states := sumBy(transitions, metricnames.AttrState)
		backend.CircuitBreaker = &CircuitBreaker{
			Opened:           states[metricnames.StateOpen],
			Closed:           states[metricnames.StateClosed],
			RejectedRequests: sumAll(rejected),
		}
		backend.CircuitBreaker.Open = backend.CircuitBreaker.Opened > backend.CircuitBreaker.Closed
	}
	if failures := index.pointsWith(metricnames.FailureRecords, metricnames.AttrBackend, inspection.Name); len(failures) > 0 {
		actions := sumBy(failures, metricnames.AttrAction)
		backend.FailureStore = &FailureStore{
			Spooled:  actions[metricnames.ActionSpooled],
			Archived: actions[metricnames.ActionArchived],
			Degraded: actions[metricnames.ActionDegraded],
			Dropped:  actions[metricnames.ActionDropped],
			Failed:   actions[metricnames.ActionFailed],
		}
	}
	return backend
}

// replayTotals は、spoolから送り直したrecordの累計を読みます。
func replayTotals(index metricIndex, backend string) ReplayTotals {
	outcomes := sumBy(index.pointsWith(metricnames.SpoolReplayRecords, metricnames.AttrBackend, backend), metricnames.AttrOutcome)
	return ReplayTotals{
		Success:    outcomes[metricnames.OutcomeSuccess],
		Retryable:  outcomes[metricnames.OutcomeRetryable],
		Dead:       outcomes[metricnames.OutcomeDead],
		Quarantine: outcomes[metricnames.OutcomeQuarantine],
	}
}

// backendsOfは、Inspectionの出力先に、metricsにだけ現れた出力先（Inspectorを持たない
// loggerの出力先など）を足して、名前の順に返します。
func backendsOf(index metricIndex, inspection Inspection) []BackendInspection {
	backends := slices.Clone(inspection.Backends)
	known := map[string]bool{}
	for _, backend := range backends {
		known[backend.Name] = true
	}
	for _, point := range index[metricnames.BackendRecords] {
		name := attrValue(point.Attrs, metricnames.AttrBackend)
		if name != "" && !known[name] {
			known[name] = true
			backends = append(backends, BackendInspection{Name: name})
		}
	}
	slices.SortStableFunc(backends, func(a, b BackendInspection) int {
		switch {
		case a.Name < b.Name:
			return -1
		case a.Name > b.Name:
			return 1
		}
		return 0
	})
	return backends
}
