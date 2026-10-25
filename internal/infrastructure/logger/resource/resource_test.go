package resource

import (
	"reflect"
	"testing"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestNewResourceAdapterOptionalCloud(t *testing.T) {
	for _, test := range []struct {
		name  string
		cloud *pkglogger.CloudResourceInfo
	}{
		{name: "omitted"},
		{name: "empty", cloud: &pkglogger.CloudResourceInfo{}},
		{
			name: "configured",
			cloud: &pkglogger.CloudResourceInfo{
				Provider: "aws", Region: "ap-northeast-1", RuntimeInstanceID: "instance-1",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := pkglogger.Resource{
				ServiceName: "service", ServiceVersion: "v1", HostName: "host",
				OSType: "linux", Environment: "test", RuntimeType: "process", Cloud: test.cloud,
			}
			before := input
			if test.cloud != nil {
				cloud := *test.cloud
				before.Cloud = &cloud
			}
			provider := NewResourceAdapter(input)
			// 構築は呼び出し元の設定とCloudの参照先を変更しません。
			if !reflect.DeepEqual(input, before) {
				t.Fatalf("adapter changed the caller's resource: got %+v (cloud %+v), want %+v (cloud %+v)",
					input, input.Cloud, before, before.Cloud)
			}
			if provider.ServiceName() != input.ServiceName || provider.ServiceVersion() != input.ServiceVersion ||
				provider.HostName() != input.HostName || provider.OSType() != input.OSType ||
				provider.Environment() != input.Environment || provider.RuntimeType() != input.RuntimeType {
				t.Fatalf("resource fields changed: %+v", provider)
			}
			var want pkglogger.CloudResourceInfo
			if test.cloud != nil {
				want = *test.cloud
				// 構築後の設定変更は、構築済みproviderへ波及させません。
				*test.cloud = pkglogger.CloudResourceInfo{Provider: "changed"}
			}
			if provider.CloudProvider() != want.Provider || provider.CloudProviderRegion() != want.Region ||
				provider.RuntimeInstanceID() != want.RuntimeInstanceID {
				t.Fatalf("cloud fields = %q/%q/%q, want %+v",
					provider.CloudProvider(), provider.CloudProviderRegion(), provider.RuntimeInstanceID(), want)
			}
		})
	}
}
