package objectstorage

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

func TestUploadConfigAdapterKeepsIndependentDestinationAndFailureSettings(t *testing.T) {
	config := LogFileUploadConfig{
		Enabled: true,
		Default: UploadTargetConfig{
			Enabled:        true,
			Targets:        []UploadTarget{UploadTargetS3},
			S3Destinations: []S3Config{{Bucket: "original"}},
			CompressedLogFileUploadFailureStore: CompressedLogFileUploadFailureStoreConfig{
				DurableType: []failurestore.DurableType{failurestore.DurableTypeSpool},
			},
		},
	}
	provider := NewLogFileUploadConfigAdapter(config)
	config.Enabled = false
	config.Default.Targets[0] = "changed"
	config.Default.S3Destinations[0].Bucket = "changed"
	config.Default.CompressedLogFileUploadFailureStore.DurableType[0] = failurestore.DurableTypeNone

	route := provider.Default()
	require.True(t, provider.Enabled())
	require.Equal(t, []UploadTarget{UploadTargetS3}, route.Targets())
	require.Equal(t, "original", route.S3DestinationConfigs()[0].Bucket())
	require.True(t, provider.UsesCompressedLogFileUploadSpool(false, false))

	// 呼び出し元が返された一覧を変更しても、次回取得する設定には影響しません。
	route.Targets()[0] = "changed"
	route.S3DestinationConfigs()[0] = NewS3ConfigAdapter(S3Config{Bucket: "changed"})
	route.CompressedLogFileUploadFailureStore().DurableType()[0] = failurestore.DurableTypeNone
	require.Equal(t, []UploadTarget{UploadTargetS3}, route.Targets())
	require.Equal(t, "original", route.S3DestinationConfigs()[0].Bucket())
	require.True(t, provider.UsesCompressedLogFileUploadSpool(false, false))
}

func TestUploadConfigAdapterPreservesS3SingleFallbackAndExplicitEmptyDestinations(t *testing.T) {
	for _, test := range []struct {
		name         string
		destinations []S3Config
		wantBuckets  []string
	}{
		{name: "nil uses single destination", wantBuckets: []string{"single"}},
		{name: "explicit empty remains empty", destinations: []S3Config{}, wantBuckets: []string{}},
		{name: "multiple replace single", destinations: []S3Config{{Bucket: "first"}, {Bucket: "second"}}, wantBuckets: []string{"first", "second"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := NewUploadTargetConfigAdapter(UploadTargetConfig{
				S3:             S3Config{Bucket: "single"},
				S3Destinations: test.destinations,
			})
			destinations := provider.S3DestinationConfigs()
			buckets := make([]string, len(destinations))
			for index, destination := range destinations {
				buckets[index] = destination.Bucket()
			}
			require.Equal(t, test.wantBuckets, buckets)
		})
	}
}
