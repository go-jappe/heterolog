package runtime

const (
	DefaultMemoryCriticalRatio           = 0.90
	DefaultMinAvailableMemoryMegaBytes   = 256
	DefaultMemoryCriticalConsecutiveHits = 3
)

type RuntimeEnv struct {
	CoreInfo   CoreRuntimeEnv
	Cloud      *CloudInfo
	Attributes map[string]string
}

type CoreRuntimeEnv struct {
	HostName          string `json:"host.name,omitempty"`
	OSType            string `json:"os.type,omitempty"`
	Environment       string `json:"deployment.environment,omitempty"` // prd / stg / dev
	RuntimeType       string `json:"runtime.kind,omitempty"`           // ecs / ec2 / k8s / docker / local
	RuntimeInstanceID string `json:"execution.unit.id,omitempty"`      // task ID / pod名 / instance ID / hostname
}

type CloudInfo struct {
	Provider string `json:"cloud.provider,omitempty"`
	Region   string `json:"cloud.region,omitempty"`
}

type RuntimeConfig struct {
	Target  string // 自身の識別子 appnameとイコールではない
	IsDebug bool

	Health RuntimeHealthConfig
}

type RuntimeHealthConfig struct {
	Memory MemoryHealthConfig
}

type MemoryHealthConfig struct {
	CriticalRatio           float64
	MinAvailableBytes       uint64
	CriticalConsecutiveHits int
}
