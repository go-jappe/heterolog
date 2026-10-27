package s3

import "github.com/go-jappe/heterolog/pkg/objectstorage"

// ConfigFromProvider builds, from each setting of provider, the input for the
// S3 client and for generating destination IDs.
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
