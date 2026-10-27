package otel

// Resource represents the resource attributes in OpenTelemetry. A resource is
// immutable metadata, per process and per service, that identifies which
// entity sent the telemetry.
type Resource struct {
	// ServiceName is the logical name of the service, such as "payment-api" or
	// "heterolog-batch". It corresponds to service.name in OpenTelemetry.
	ServiceName string

	// ServiceVersion is the version of the deployed service, such as "1.2.3" or
	// "2026-02-25". It is used for identification when looking into differences
	// between deployments or investigating defects.
	ServiceVersion string

	// CloudProvider is the name of the cloud provider, such as "aws", "gcp",
	// "azure" or "on-prem". It is used for visualization and analysis per cloud
	// environment.
	CloudProvider string

	// HostName is the name of the execution host, such as an EC2 instance name
	// or a container host name. It is used for failure and load analysis per
	// host.
	HostName string

	// OSType is the OS type of the runtime environment, such as "linux",
	// "darwin" or "windows". It is used to analyze environment-dependent
	// problems.
	OSType string

	// Environment is the category of the runtime environment, such as
	// "production", "staging" or "development". It is used for filtering and
	// separation by environment.
	Environment string
}
