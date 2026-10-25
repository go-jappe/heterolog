package resource

import (
	"github.com/go-jappe/heterolog/pkg/otel"
)

type OtelLogResourceProvider interface {
	ServiceName() string
	ServiceVersion() string
	CloudProvider() string
	HostName() string
	OSType() string
	Environment() string
}

func NewOtelResourceAdapter(r *otel.Resource) OtelLogResourceProvider {
	return &OtelResourceAdapter{
		serviceName:    r.ServiceName,
		serviceVersion: r.ServiceVersion,
		cloudProvider:  r.CloudProvider,
		hostName:       r.HostName,
		osType:         r.OSType,
		environment:    r.Environment,
	}
}

type OtelResourceAdapter struct {
	serviceName    string
	serviceVersion string
	cloudProvider  string
	hostName       string
	osType         string
	environment    string // dev / stg / prod
}

func (r *OtelResourceAdapter) ServiceName() string {
	return r.serviceName
}
func (r *OtelResourceAdapter) ServiceVersion() string {
	return r.serviceVersion
}
func (r *OtelResourceAdapter) HostName() string {
	return r.hostName
}
func (r *OtelResourceAdapter) OSType() string {
	return r.osType
}
func (r *OtelResourceAdapter) Environment() string {
	return r.environment
}
func (r *OtelResourceAdapter) CloudProvider() string {
	return r.cloudProvider
}
