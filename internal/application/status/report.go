// Package statusは、稼働中のloggerとtelemetryの様子（processのCPUとmemory、出力先ごとの
// 送信の結果と所要時間、spoolとdirectoryの使用状況）を一つの報告にまとめ、注意すべき状態を
// 判定します。管理用APIのGET /statusの処理です。値の出どころのうち、metricsは各memberの
// 内部のMeterから読み、spoolやdirectoryの場所はinfrastructure層のInspectorから受け取り、
// fileの数とdisk・CPUの読み取りはinfrastructure層のProbesに任せます。
package status

import "time"

// Reportは、一回のGET /statusの報告です。configview.Renderで、snake_caseのkeyのJSONに
// します。
type Report struct {
	GeneratedAt time.Time
	Process     Process
	Members     map[string]MemberReport
	// Directoriesは、memberが使うlog・spool・failure archiveのdirectoryです。
	Directories []Directory
	// Warningsは、注意すべき状態で、criticalを先に並べます。
	Warnings []Warning
}

// Processは、このprocessの様子です。
type Process struct {
	StartedAt     time.Time
	UptimeSeconds float64
	Goroutines    int
	MaxProcs      int
	// CPUは、CPUの時間を読めない環境ではnilです。
	CPU *CPU
	// Memoryは、memoryを読めない環境ではnilです。
	Memory *Memory
}

// CPUは、processのCPUの使用です。Percentは、直近のWindowSecondsの間にprocessが使った
// CPU時間の割合で、1つのcoreを使い切ると100です。
type CPU struct {
	Percent       float64
	UserSeconds   float64
	SystemSeconds float64
	WindowSeconds float64
}

// Memoryは、processとhost（またはcgroup）のmemoryです。RSSBytes・HeapBytes・SysBytesは
// このprocessの値、UsedBytes以下はmemory pressure monitorと同じ読み取りです。
type Memory struct {
	RSSBytes       uint64
	HeapBytes      uint64
	SysBytes       uint64
	UsedBytes      uint64
	LimitBytes     uint64
	AvailableBytes uint64
	Ratio          float64
	Source         string
	Reliable       bool
	Container      bool
}

// MemberReportは、一つのloggerかtelemetryの様子です。
type MemberReport struct {
	Kind     string
	Backends []Backend
}

// Backendは、一つの出力先（telemetryでは送り先）の様子です。数は、processの起動からの
// 累計です。
type Backend struct {
	Name    string
	Records Records
	// Requestsは、送信を記録していない出力先（stdoutなど）ではnilです。
	Requests *Requests
	Retries  int64
	// CircuitBreakerは、circuit breakerを使わない出力先ではnilです。
	CircuitBreaker *CircuitBreaker
	// FailureStoreは、送れなかったrecordを扱っていなければnilです。
	FailureStore *FailureStore
	// Spoolは、spoolを使わない出力先ではnilです。
	Spool *Spool
}

// Recordsは、出力先へ送ったrecordの数を、結果ごとに数えたものです。
type Records struct {
	Success    int64
	Retryable  int64
	Dead       int64
	Quarantine int64
	Failure    int64
	Dropped    int64
}

// Requestsは、出力先への送信の回数と所要時間です。所要時間は、再試行とその間の待ち時間を
// 含みます。
type Requests struct {
	Count          int64
	Failures       int64
	AverageSeconds float64
	MaxSeconds     float64
}

// CircuitBreakerは、circuit breakerの状態です。Openは、開いた回数が閉じた回数より多い
// ことです。
type CircuitBreaker struct {
	Open             bool
	Opened           int64
	Closed           int64
	RejectedRequests int64
}

// FailureStoreは、送れなかったrecordの行き先ごとの数です。
type FailureStore struct {
	Spooled  int64
	Archived int64
	Degraded int64
	Dropped  int64
	Failed   int64
}

// Spoolは、出力先のspoolの様子です。
type Spool struct {
	Root string
	// Statesは、active・processing・done・dead・quarantineのdirectoryの使用状況です。
	States       map[string]DirectoryUsage
	Replay       Replay
	ReplayTotals ReplayTotals
	// MemoryCriticalは、spoolのmemory逼迫の監視の最後の判定です。監視を使わない場合は
	// nilです。
	MemoryCritical *bool
}

// Replayは、spoolから送り直すrunnerの状態です。
type Replay struct {
	// Stateは、idle・running・restarting・stoppedのいずれかです。
	State        string
	StartedAt    time.Time
	LastRunAt    time.Time
	LastPurgeAt  time.Time
	RestartCount int
	LastError    string
	LastRun      ReplayRun
}

// ReplayRunは、最後の送り直しの周期の結果です。
type ReplayRun struct {
	Records    int
	Success    int
	Dead       int
	Quarantine int
	Retryable  int
}

// ReplayTotalsは、spoolから送り直したrecordの累計です。
type ReplayTotals struct {
	Success    int64
	Retryable  int64
	Dead       int64
	Quarantine int64
}

// DirectoryUsageは、一つのdirectoryのfileの数とbyte数です。定期scanの結果で、ScannedAtが
// zero valueならまだscanしていないか、scanを無効にしています。
type DirectoryUsage struct {
	Files     int
	Bytes     uint64
	ScannedAt time.Time
	// Errorは、最後のscanの失敗です。
	Error string
}

// Directoryは、memberが使うdirectoryと、その上限とdiskの空きです。
type Directory struct {
	Path string
	// Roleは、log・error_log・debug_log・spool・failure_archiveなどです。
	Role    string
	Member  string
	Backend string
	Usage   DirectoryUsage
	// LimitBytesは、設定したdirectoryの大きさの上限で、0なら上限なしです。
	LimitBytes uint64
	// MinFreeBytesは、設定したdiskの空きの下限で、0なら下限なしです。
	MinFreeBytes uint64
	// Diskは、directoryのあるfile systemの空きと総量です。読めなければnilです。
	Disk *Disk
}

// Diskは、file systemの空きと総量です。
type Disk struct {
	FreeBytes  uint64
	TotalBytes uint64
}

// Warningの重さです。
const (
	SeverityWarning  = "warning"
	SeverityCritical = "critical"
)

// Warningは、注意すべき状態です。Codeは状態の種類、Targetはmemberと出力先（app/opensearch）
// かdirectoryのpathです。
type Warning struct {
	Severity string
	Code     string
	Target   string
	Message  string
}
