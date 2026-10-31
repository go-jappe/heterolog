package status

import (
	"context"
	"time"

	"github.com/go-jappe/heterolog/internal/application/metrics"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
)

// Memberは、statusを報告できるloggerかtelemetryです。heterologが作るruntimeが実装します。
type Member interface {
	Status(ctx context.Context) MemberStatus
}

// MemberStatusは、一つのmemberの材料です。Metricsは内部のMeterの累積値、Inspectionは
// infrastructure層が示すspoolとdirectoryです。
type MemberStatus struct {
	Kind       string
	Metrics    []metrics.Metric
	Inspection Inspection
}

// Inspectorは、loggerやtelemetryの配送の仕組みのうち、spoolとdirectoryを示せるものです。
// infrastructure層が実装し、runtimeがMemberStatus.Inspectionに入れます。
type Inspector interface {
	InspectStatus(ctx context.Context) Inspection
}

// Inspectionは、Inspectorが示す出力先とdirectoryです。
type Inspection struct {
	Backends    []BackendInspection
	Directories []DirectoryInspection
}

// Signalは、telemetryの送り先が扱うdataの種類です。metricsの名前を選ぶために使います。
// loggerの出力先はSignalLogsです。
const (
	SignalLogs    = ""
	SignalTraces  = "traces"
	SignalMetrics = "metrics"
)

// BackendInspectionは、一つの出力先です。Nameはmetricsのbackend属性の値で、telemetryの
// 送り先ではExporterにexporter属性の値も示します。
type BackendInspection struct {
	Name     string
	Exporter string
	Signal   string
	Spool    *SpoolInspection
}

// SpoolInspectionは、出力先のspoolの場所と、送り直すrunnerの状態です。StateDirsは、
// 状態（active・processing・done・dead・quarantine）ごとのdirectoryです。
type SpoolInspection struct {
	Root      string
	StateDirs map[string]string
	Replay    Replay
	// ActiveLimitBytesは、activeのdirectoryの大きさの上限（overloadの判定）で、0なら
	// 上限なしです。MinFreeBytesは、diskの空きの下限で、0なら下限なしです。
	ActiveLimitBytes uint64
	MinFreeBytes     uint64
	MemoryCritical   *bool
}

// SpoolInspectorは、spoolの様子を示せるfailure storeです。telemetryの送り先のfailure
// storeが実装します。
type SpoolInspector interface {
	SpoolInspection(ctx context.Context) *SpoolInspection
}

// DirectoryInspectionは、memberが使う一つのdirectoryです。Roleは、log・error_log・
// debug_log・failure_archiveなどです。
type DirectoryInspection struct {
	Path         string
	Role         string
	Backend      string
	LimitBytes   uint64
	MinFreeBytes uint64
}

// ProcessProbeは、このprocessのCPUの時間を読みます。infrastructure層がOSごとに実装します。
type ProcessProbe interface {
	CPUTimes() (user, system time.Duration, err error)
}

// Usageは、一つのdirectoryのfileの数とbyte数です。
type Usage struct {
	Files int
	Bytes uint64
}

// Scannerは、directoryのfileを数え、diskの空きを読みます。infrastructure層が実装します。
type Scanner interface {
	// ScanDirectoryは、path以下の通常のfileの数とbyte数を返します。pathがなければ0です。
	ScanDirectory(ctx context.Context, path string) (Usage, error)
	// DiskSpaceは、pathのあるfile systemの空きと総量を返します。
	DiskSpace(path string) (free, total uint64, err error)
}

// Probesは、Serviceが使うinfrastructure層の読み取りです。nilの項目は、その値を報告しません。
type Probes struct {
	Process ProcessProbe
	Memory  memorypressure.Provider
	Scanner Scanner
}
