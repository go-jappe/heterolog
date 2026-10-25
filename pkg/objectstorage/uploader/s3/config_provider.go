package s3

import "github.com/go-jappe/heterolog/pkg/objectstorage"

// ConfigFromProviderは、adapterの各設定値からS3 client・宛先ID生成用の入力を作ります。
func ConfigFromProvider(provider objectstorage.S3ConfigProvider) objectstorage.S3Config {
	return objectstorage.S3Config{
		Region:          provider.Region(),
		Endpoint:        provider.Endpoint(),
		AccessKeyID:     provider.AccessKeyID(),
		SecretAccessKey: provider.SecretAccessKey(),
		SessionToken:    provider.SessionToken(),
		Bucket:          provider.Bucket(),
		Prefix:          provider.Prefix(),
		ForcePathStyle:  provider.ForcePathStyle(),
		Timeout:         provider.Timeout(),
	}
}
