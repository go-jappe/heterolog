package status

import (
	"context"
	"maps"
	"runtime"
	runtimemetrics "runtime/metrics"
	"slices"
	"sync"
	"time"
)

// Configは、Serviceの設定です。
type Config struct {
	// ScanIntervalは、directoryのfileを数える定期scanの間隔です。0以下ならscanしません。
	// scanは、GET /statusの処理で間隔が過ぎていたら背景で始め、その回の報告には前回の
	// 結果を使います。
	ScanInterval time.Duration
	Thresholds   Thresholds
	// Nowは、試験で時刻を差し替えるためのものです。nilならtime.Nowです。
	Now func() time.Time
}

// cpuWindowは、CPUの使用率を出す区間の下限です。これより短い間隔のGET /statusは、前回の
// 使用率を返します。
const cpuWindow = time.Second

// Serviceは、memberの様子を集めて報告します。
type Service struct {
	members map[string]Member
	probes  Probes
	cfg     Config
	now     func() time.Time

	startedAt time.Time

	mu  sync.Mutex
	cpu *cpuSample
	// scansは、pathごとの定期scanの結果です。
	scans map[string]scanEntry
	// scanningは、背景のscanが動いている間trueです。
	scanning bool
	// scanDoneは、背景のscanが終わると閉じます。試験が待つために使います。
	scanDone chan struct{}
}

type cpuSample struct {
	total   time.Duration
	at      time.Time
	percent float64
	window  time.Duration
}

type scanEntry struct {
	usage     Usage
	scannedAt time.Time
	err       error
}

// NewServiceは、membersの様子をprobesで集めるServiceを返します。
func NewService(members map[string]Member, probes Probes, cfg Config) *Service {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	if cfg.Thresholds == (Thresholds{}) {
		cfg.Thresholds = DefaultThresholds()
	}
	return &Service{
		members:   maps.Clone(members),
		probes:    probes,
		cfg:       cfg,
		now:       now,
		startedAt: now(),
		scans:     map[string]scanEntry{},
	}
}

// Reportは、今の様子を報告します。directoryのfileの数は、前回のscanの結果で、間隔が
// 過ぎていれば背景で次のscanを始めます。
func (s *Service) Report(ctx context.Context) Report {
	now := s.now()
	report := Report{GeneratedAt: now, Members: make(map[string]MemberReport, len(s.members))}
	report.Process = s.process(ctx, now)
	var paths []string
	for _, name := range slices.Sorted(maps.Keys(s.members)) {
		member := s.members[name].Status(ctx)
		index := indexMetrics(member.Metrics)
		result := MemberReport{Kind: member.Kind}
		for _, inspection := range backendsOf(index, member.Inspection) {
			backend := deriveBackend(index, inspection)
			if inspection.Spool != nil {
				spool, directories, spoolPaths := s.spool(name, inspection, index)
				backend.Spool = spool
				report.Directories = append(report.Directories, directories...)
				paths = append(paths, spoolPaths...)
			}
			result.Backends = append(result.Backends, backend)
		}
		for _, directory := range member.Inspection.Directories {
			report.Directories = append(report.Directories, Directory{
				Path:         directory.Path,
				Role:         directory.Role,
				Member:       name,
				Backend:      directory.Backend,
				Usage:        s.usage(directory.Path),
				LimitBytes:   directory.LimitBytes,
				MinFreeBytes: directory.MinFreeBytes,
				Disk:         s.disk(directory.Path),
			})
			paths = append(paths, directory.Path)
		}
		report.Members[name] = result
	}
	s.refreshScans(paths, now)
	report.Warnings = Evaluate(&report, s.cfg.Thresholds)
	return report
}

// spoolは、spoolの報告と、spoolのdirectory（rootとactive）の報告を作ります。rootの使用
// 状況は、状態ごとのdirectoryのscanの合計です。
func (s *Service) spool(member string, inspection BackendInspection, index metricIndex) (*Spool, []Directory, []string) {
	in := inspection.Spool
	spool := &Spool{
		Root:           in.Root,
		States:         make(map[string]DirectoryUsage, len(in.StateDirs)),
		Replay:         in.Replay,
		ReplayTotals:   replayTotals(index, inspection.Name),
		MemoryCritical: in.MemoryCritical,
	}
	var paths []string
	total := DirectoryUsage{}
	for _, state := range slices.Sorted(maps.Keys(in.StateDirs)) {
		usage := s.usage(in.StateDirs[state])
		spool.States[state] = usage
		paths = append(paths, in.StateDirs[state])
		total.Files += usage.Files
		total.Bytes += usage.Bytes
		if usage.ScannedAt.After(total.ScannedAt) {
			total.ScannedAt = usage.ScannedAt
		}
		if usage.Error != "" && total.Error == "" {
			total.Error = usage.Error
		}
	}
	directories := []Directory{{
		Path: in.Root, Role: RoleSpool, Member: member, Backend: inspection.Name,
		Usage: total, MinFreeBytes: in.MinFreeBytes, Disk: s.disk(in.Root),
	}}
	if active, ok := in.StateDirs[SpoolStateActive]; ok {
		directories = append(directories, Directory{
			Path: active, Role: RoleSpoolActive, Member: member, Backend: inspection.Name,
			Usage: spool.States[SpoolStateActive], LimitBytes: in.ActiveLimitBytes, MinFreeBytes: in.MinFreeBytes,
		})
	}
	return spool, directories, paths
}

// directoryのroleと、spoolの状態の名前です。
const (
	RoleSpool       = "spool"
	RoleSpoolActive = "spool_active"

	SpoolStateActive = "active"
)

func (s *Service) usage(path string) DirectoryUsage {
	s.mu.Lock()
	entry, ok := s.scans[path]
	s.mu.Unlock()
	if !ok {
		return DirectoryUsage{}
	}
	usage := DirectoryUsage{Files: entry.usage.Files, Bytes: entry.usage.Bytes, ScannedAt: entry.scannedAt}
	if entry.err != nil {
		usage.Error = entry.err.Error()
	}
	return usage
}

func (s *Service) disk(path string) *Disk {
	if s.probes.Scanner == nil {
		return nil
	}
	free, total, err := s.probes.Scanner.DiskSpace(path)
	if err != nil {
		return nil
	}
	return &Disk{FreeBytes: free, TotalBytes: total}
}

// refreshScansは、scanの間隔が過ぎたpathがあれば、背景でscanを始めます。scanは一度に
// 一つだけ動かし、pathを順にscanします。
func (s *Service) refreshScans(paths []string, now time.Time) {
	if s.cfg.ScanInterval <= 0 || s.probes.Scanner == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.scanning {
		return
	}
	var stale []string
	seen := map[string]bool{}
	for _, path := range paths {
		if seen[path] {
			continue
		}
		seen[path] = true
		entry, ok := s.scans[path]
		if !ok || now.Sub(entry.scannedAt) >= s.cfg.ScanInterval {
			stale = append(stale, path)
		}
	}
	if len(stale) == 0 {
		return
	}
	s.scanning = true
	done := make(chan struct{})
	s.scanDone = done
	go func() {
		defer close(done)
		s.scan(context.Background(), stale)
		s.mu.Lock()
		s.scanning = false
		s.mu.Unlock()
	}()
}

// scanは、pathsを順にscanし、結果を保存します。
func (s *Service) scan(ctx context.Context, paths []string) {
	for _, path := range paths {
		usage, err := s.probes.Scanner.ScanDirectory(ctx, path)
		s.mu.Lock()
		s.scans[path] = scanEntry{usage: usage, scannedAt: s.now(), err: err}
		s.mu.Unlock()
	}
}

// ScanNowは、前回の報告で知ったdirectoryをすべてscanし、終わるまで待ちます。
func (s *Service) ScanNow(ctx context.Context) {
	if s.probes.Scanner == nil {
		return
	}
	s.mu.Lock()
	paths := slices.Sorted(maps.Keys(s.scans))
	s.mu.Unlock()
	s.scan(ctx, paths)
}

// processは、このprocessの様子を読みます。
func (s *Service) process(ctx context.Context, now time.Time) Process {
	process := Process{
		StartedAt:     s.startedAt,
		UptimeSeconds: now.Sub(s.startedAt).Seconds(),
		Goroutines:    runtime.NumGoroutine(),
		MaxProcs:      runtime.GOMAXPROCS(0),
		CPU:           s.cpuUsage(now),
		Memory:        s.memory(ctx),
	}
	return process
}

// cpuUsageは、前回の読み取りからのCPU時間の増分で、使用率を出します。初回は起動からの
// 平均です。cpuWindowより短い間隔では、前回の使用率を返します。
func (s *Service) cpuUsage(now time.Time) *CPU {
	if s.probes.Process == nil {
		return nil
	}
	user, system, err := s.probes.Process.CPUTimes()
	if err != nil {
		return nil
	}
	total := user + system
	s.mu.Lock()
	defer s.mu.Unlock()
	sample := cpuSample{total: total, at: now}
	switch {
	case s.cpu == nil:
		sample.window = now.Sub(s.startedAt)
		sample.percent = percentOf(total, sample.window)
	case now.Sub(s.cpu.at) >= cpuWindow:
		sample.window = now.Sub(s.cpu.at)
		sample.percent = percentOf(total-s.cpu.total, sample.window)
	default:
		sample.window, sample.percent = s.cpu.window, s.cpu.percent
		sample.at, sample.total = s.cpu.at, s.cpu.total
	}
	s.cpu = &sample
	return &CPU{
		Percent:       sample.percent,
		UserSeconds:   user.Seconds(),
		SystemSeconds: system.Seconds(),
		WindowSeconds: sample.window.Seconds(),
	}
}

func percentOf(used, window time.Duration) float64 {
	if window <= 0 {
		return 0
	}
	return float64(used) / float64(window) * 100
}

// memoryは、Goのruntimeのheapと、memory pressure monitorと同じ読み取りを返します。
func (s *Service) memory(ctx context.Context) *Memory {
	samples := []runtimemetrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/total:bytes"},
	}
	runtimemetrics.Read(samples)
	memory := &Memory{Source: "unknown"}
	if samples[0].Value.Kind() == runtimemetrics.KindUint64 {
		memory.HeapBytes = samples[0].Value.Uint64()
	}
	if samples[1].Value.Kind() == runtimemetrics.KindUint64 {
		memory.SysBytes = samples[1].Value.Uint64()
	}
	if s.probes.Memory == nil {
		return memory
	}
	pressure, err := s.probes.Memory.ReadMemoryPressure(ctx)
	if err != nil {
		return memory
	}
	memory.RSSBytes = pressure.RSSBytes
	memory.UsedBytes = pressure.UsedBytes
	memory.LimitBytes = pressure.LimitBytes
	memory.AvailableBytes = pressure.AvailableBytes
	memory.Ratio = pressure.Ratio
	memory.Source = string(pressure.Source)
	memory.Reliable = pressure.Reliable
	memory.Container = pressure.Container
	return memory
}
