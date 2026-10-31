package status

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/metricnames"
	"github.com/go-jappe/heterolog/internal/application/metrics"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

type memberForTest MemberStatus

func (m memberForTest) Status(context.Context) MemberStatus { return MemberStatus(m) }

// probesForTestは、決めた値を返す読み取りです。
type probesForTest struct {
	cpu   time.Duration
	usage map[string]Usage
	free  uint64
	scans int
}

func (p *probesForTest) CPUTimes() (time.Duration, time.Duration, error) { return p.cpu, 0, nil }

func (p *probesForTest) ReadMemoryPressure(context.Context) (memorypressure.MemoryPressure, error) {
	return memorypressure.MemoryPressure{RSSBytes: 1 << 20, UsedBytes: 3 << 30, LimitBytes: 4 << 30, Ratio: 0.75, Source: "test", Reliable: true}, nil
}

func (p *probesForTest) ScanDirectory(_ context.Context, path string) (Usage, error) {
	p.scans++
	usage, ok := p.usage[path]
	if !ok {
		return Usage{}, errors.New("unreadable")
	}
	return usage, nil
}

func (p *probesForTest) DiskSpace(string) (uint64, uint64, error) { return p.free, 100, nil }

func attrs(pairs ...string) []pkgobserver.Attr {
	var result []pkgobserver.Attr
	for i := 0; i+1 < len(pairs); i += 2 {
		result = append(result, pkgobserver.Attr{Key: pairs[i], Value: pairs[i+1]})
	}
	return result
}

func loggerMetrics(t *testing.T) []metrics.Metric {
	t.Helper()
	ctx := context.Background()
	meter := metrics.NewMeter(metrics.Config{}, nil)
	records := meter.Counter(metricnames.BackendRecords)
	records.Add(ctx, 90, attrs(metricnames.AttrBackend, "opensearch", metricnames.AttrOutcome, metricnames.OutcomeSuccess)...)
	records.Add(ctx, 20, attrs(metricnames.AttrBackend, "opensearch", metricnames.AttrOutcome, metricnames.OutcomeRetryable)...)
	records.Add(ctx, 5, attrs(metricnames.AttrBackend, "stdout", metricnames.AttrOutcome, metricnames.OutcomeSuccess)...)
	duration := meter.Histogram(metricnames.BackendRequestDuration)
	duration.Record(ctx, 2, attrs(metricnames.AttrBackend, "opensearch", metricnames.AttrOutcome, metricnames.OutcomeSuccess)...)
	duration.Record(ctx, 12, attrs(metricnames.AttrBackend, "opensearch", metricnames.AttrOutcome, metricnames.OutcomeFailure)...)
	meter.Counter(metricnames.BackendRetries).Add(ctx, 4, attrs(metricnames.AttrBackend, "opensearch", metricnames.AttrReason, "status")...)
	meter.Counter(metricnames.CircuitBreakerTransitions).Add(ctx, 1, attrs(metricnames.AttrBackend, "opensearch", metricnames.AttrState, metricnames.StateOpen)...)
	meter.Counter(metricnames.FailureRecords).Add(ctx, 20, attrs(metricnames.AttrBackend, "opensearch", metricnames.AttrAction, metricnames.ActionSpooled)...)
	meter.Counter(metricnames.SpoolReplayRecords).Add(ctx, 7, attrs(metricnames.AttrBackend, "opensearch", metricnames.AttrOutcome, metricnames.OutcomeSuccess)...)
	return meter.Collect()
}

func TestReportDerivesBackendsSpoolsDirectoriesAndWarnings(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	probes := &probesForTest{cpu: 500 * time.Millisecond, free: 4, usage: map[string]Usage{
		"/spool/opensearch/active": {Files: 3, Bytes: 300}, "/spool/opensearch/dead": {Files: 1, Bytes: 10},
		"/spool/opensearch/done": {}, "/logs": {Files: 10, Bytes: 95},
	}}
	critical := true
	member := memberForTest{Kind: "logger", Metrics: loggerMetrics(t), Inspection: Inspection{
		Backends: []BackendInspection{{Name: "opensearch", Spool: &SpoolInspection{
			Root:      "/spool/opensearch",
			StateDirs: map[string]string{"active": "/spool/opensearch/active", "dead": "/spool/opensearch/dead", "done": "/spool/opensearch/done"},
			Replay:    Replay{State: "stopped", LastError: "boom"}, MemoryCritical: &critical, ActiveLimitBytes: 1000,
		}}},
		Directories: []DirectoryInspection{{Path: "/logs", Role: "log", LimitBytes: 100, MinFreeBytes: 10}},
	}}
	service := NewService(map[string]Member{"app": member}, Probes{Process: probes, Memory: probes, Scanner: probes},
		Config{ScanInterval: time.Minute, Now: clock})

	// 初回の報告はscanを背景で始め、fileの数はまだありません。
	now = now.Add(10 * time.Second)
	report := service.Report(context.Background())
	<-service.scanDone
	if got := report.Members["app"].Backends[0].Spool.States["active"]; !got.ScannedAt.IsZero() {
		t.Errorf("first report active = %+v, want not scanned yet", got)
	}
	if probes.scans != 4 {
		t.Errorf("scans = %d, want 4 (three spool states and the log directory)", probes.scans)
	}
	// 2回目は、scanの結果と注意を含みます。
	now = now.Add(10 * time.Second)
	report = service.Report(context.Background())
	if probes.scans != 4 {
		t.Errorf("scans after the second report = %d, want still 4 within the interval", probes.scans)
	}
	backends := report.Members["app"].Backends
	if len(backends) != 2 || backends[0].Name != "opensearch" || backends[1].Name != "stdout" {
		t.Fatalf("backends = %+v, want opensearch (inspected) and stdout (from the metrics)", backends)
	}
	os := backends[0]
	if os.Records != (Records{Success: 90, Retryable: 20}) || os.Retries != 4 ||
		os.Requests == nil || os.Requests.Count != 2 || os.Requests.Failures != 1 || os.Requests.AverageSeconds != 7 || os.Requests.MaxSeconds != 12 ||
		os.CircuitBreaker == nil || !os.CircuitBreaker.Open || os.FailureStore == nil || os.FailureStore.Spooled != 20 {
		t.Errorf("opensearch = %+v (requests %+v, breaker %+v, failure store %+v), want the derived values", os, os.Requests, os.CircuitBreaker, os.FailureStore)
	}
	if os.Spool == nil || os.Spool.States["active"].Files != 3 || os.Spool.States["dead"].Bytes != 10 || os.Spool.ReplayTotals.Success != 7 || os.Spool.Replay.State != "stopped" {
		t.Errorf("opensearch spool = %+v, want the scanned states and the replay state", os.Spool)
	}
	if backends[1].Records.Success != 5 || backends[1].Requests != nil || backends[1].Spool != nil {
		t.Errorf("stdout = %+v, want only 5 records", backends[1])
	}
	var roles []string
	for _, directory := range report.Directories {
		roles = append(roles, directory.Role+":"+directory.Path)
	}
	if want := []string{"spool:/spool/opensearch", "spool_active:/spool/opensearch/active", "log:/logs"}; !slices.Equal(roles, want) {
		t.Errorf("directories = %v, want %v", roles, want)
	}
	if root := report.Directories[0]; root.Usage.Files != 4 || root.Usage.Bytes != 310 || root.Disk == nil || root.Disk.FreeBytes != 4 {
		t.Errorf("spool root = %+v, want the sum of the states and the disk space", root)
	}
	if logs := report.Directories[2]; logs.Usage.Bytes != 95 || logs.LimitBytes != 100 {
		t.Errorf("log directory = %+v", logs)
	}
	var codes []string
	for _, warning := range report.Warnings {
		codes = append(codes, warning.Severity+":"+warning.Code+":"+warning.Target)
	}
	want := []string{
		"critical:" + CodeDiskBelowMinFree + ":/logs",
		"critical:" + CodeDiskFreeCritical + ":/logs",
		"critical:" + CodeDiskFreeCritical + ":/spool/opensearch",
		"critical:" + CodeCircuitBreakerOpen + ":app/opensearch",
		"critical:" + CodeSpoolMemoryCritical + ":app/opensearch",
		"critical:" + CodeSpoolReplayStopped + ":app/opensearch",
		"warning:" + CodeDirectoryNearLimit + ":/logs",
		"warning:" + CodeBackendFailures + ":app/opensearch",
		"warning:" + CodeBackendSlow + ":app/opensearch",
		"warning:" + CodeSpoolFailedRecords + ":app/opensearch",
	}
	if !slices.Equal(codes, want) {
		t.Errorf("warnings =\n%v\nwant\n%v", codes, want)
	}
	// processの値です。CPUは、初回が起動からの平均、以降は前回からの増分です。
	if p := report.Process; p.CPU == nil || p.Memory == nil || p.Memory.Ratio != 0.75 || p.Memory.HeapBytes == 0 || p.UptimeSeconds != 20 {
		t.Errorf("process = %+v (cpu %+v, memory %+v)", p, p.CPU, p.Memory)
	}
	probes.cpu = 1500 * time.Millisecond
	now = now.Add(2 * time.Second)
	if cpu := service.Report(context.Background()).Process.CPU; cpu.Percent != 50 || cpu.WindowSeconds != 2 {
		t.Errorf("cpu = %+v, want 50%% over 2s", cpu)
	}
}

func TestReportWithoutScanningAndProbes(t *testing.T) {
	member := memberForTest{Kind: "telemetry", Inspection: Inspection{Backends: []BackendInspection{
		{Name: "otlp_traces", Exporter: "otlp", Signal: SignalTraces, Spool: &SpoolInspection{Root: "/spool/t", StateDirs: map[string]string{"active": "/spool/t/active"}, Replay: Replay{State: "running"}}},
	}}}
	service := NewService(map[string]Member{"tel": member}, Probes{}, Config{})
	report := service.Report(context.Background())
	if report.Process.CPU != nil || report.Process.Memory == nil || report.Process.Memory.Source != "unknown" {
		t.Errorf("process = %+v, want no CPU and only the Go memory", report.Process)
	}
	backend := report.Members["tel"].Backends[0]
	if backend.Spool == nil || backend.Spool.States["active"] != (DirectoryUsage{}) || report.Directories[0].Disk != nil {
		t.Errorf("backend = %+v, want an unscanned spool without disk space", backend)
	}
	if len(report.Warnings) != 0 {
		t.Errorf("warnings = %+v, want none without data", report.Warnings)
	}
}
