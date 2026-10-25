package resource

import "github.com/go-jappe/heterolog/pkg/logger"

type ResourceProvider interface {
	ServiceName() string
	ServiceVersion() string
	HostName() string
	OSType() string
	Environment() string
	RuntimeType() string
	CloudProvider() string
	CloudProviderRegion() string
	RuntimeInstanceID() string
}

// NewResourceAdapterはResourceを共通の参照用設定へ変換します。
// Cloudの省略はクラウド情報なしとして扱い、元の設定は変更しません。
func NewResourceAdapter(r logger.Resource) ResourceProvider {
	var cloud cloudInfo
	if r.Cloud != nil {
		cloud = cloudInfo{
			provider:          r.Cloud.Provider,
			region:            r.Cloud.Region,
			runtimeInstanceID: r.Cloud.RuntimeInstanceID,
		}
	}
	return ResourceAdapter{
		serviceName:    r.ServiceName,
		serviceVersion: r.ServiceVersion,
		runtimeType:    r.RuntimeType,
		hostName:       r.HostName,
		osType:         r.OSType,
		environment:    r.Environment,
		cloud:          cloud,
	}
}

type ResourceAdapter struct {
	serviceName    string // appname
	serviceVersion string
	hostName       string
	osType         string
	runtimeType    string
	environment    string // dev / stg / prod
	cloud          cloudInfo
}

type cloudInfo struct {
	provider          string
	region            string
	runtimeInstanceID string
}

func (r ResourceAdapter) ServiceName() string {
	return r.serviceName
}
func (r ResourceAdapter) ServiceVersion() string {
	return r.serviceVersion
}
func (r ResourceAdapter) HostName() string {
	return r.hostName
}
func (r ResourceAdapter) OSType() string {
	return r.osType
}
func (r ResourceAdapter) Environment() string {
	return r.environment
}
func (r ResourceAdapter) RuntimeType() string {
	return r.runtimeType
}
func (r ResourceAdapter) CloudProvider() string {
	return r.cloud.provider
}
func (r ResourceAdapter) CloudProviderRegion() string {
	return r.cloud.region
}
func (r ResourceAdapter) RuntimeInstanceID() string {
	return r.cloud.runtimeInstanceID
}
