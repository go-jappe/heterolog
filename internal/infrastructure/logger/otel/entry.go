package otel

type OtelLogEntry struct {
	Timestamp      string        `json:"timestamp"`
	SeverityText   string        `json:"severity_text"`
	SeverityNumber int           `json:"severity_number"`
	Body           string        `json:"body"`
	Attributes     LogAttributes `json:"attributes,omitempty"`
	Resource       *LogResource  `json:"resource,omitempty"` // 省略可能
}

type LogAttributes struct {
	TraceID             string         `json:"trace_id,omitempty"`
	SpanID              string         `json:"span_id,omitempty"`
	RequestId           string         `json:"request_id,omitempty"`
	ExceptionMessage    string         `json:"exception.message,omitempty"`
	ExceptionStacktrace string         `json:"exception.stacktrace,omitempty"`
	InternalError       bool           `json:"internal_error,omitempty"`
	Source              string         `json:"source,omitempty"`
	Fields              map[string]any `json:"fields,omitempty"`
}

type LogResource struct {
	ServiceName    string `json:"service.name,omitempty"`
	ServiceVersion string `json:"service.version,omitempty"`
	CloudProvider  string `json:"cloud.provider,omitempty"`
	HostName       string `json:"host.name,omitempty"`
	OSType         string `json:"os.type,omitempty"`
	Environment    string `json:"deployment.environment,omitempty"`
	//	CloudRegion           string `json:"cloud.region,omitempty"`
	//	CloudAccountID        string `json:"cloud.account.id,omitempty"`
	//	CloudInstanceID       string `json:"cloud.instance.id,omitempty"`
	//	CloudServiceName      string `json:"cloud.service.name,omitempty"`
	// AwsEcsClusterName     string `json:"aws.ecs.cluster.name,omitempty"`
	// AwsEcsServiceName     string `json:"aws.ecs.service.name,omitempty"`
	// AwsEcsTaskArn         string `json:"aws.ecs.task.arn,omitempty"`
	// AwsEcsTaskDefFamily   string `json:"aws.ecs.task.definition.family,omitempty"`
	// AwsEcsTaskDefRevision string `json:"aws.ecs.task.definition.revision,omitempty"`
	// K8sNamespaceName      string `json:"k8s.namespace.name,omitempty"`
	// K8sPodName            string `json:"k8s.pod.name,omitempty"`
	// ContainerName         string `json:"container.name,omitempty"`
}
