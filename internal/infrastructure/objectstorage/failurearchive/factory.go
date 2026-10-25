// Package failurearchive はfailure archive用object storage uploadを構成します。
package failurearchive

import (
	"slices"

	fsfailurearchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/uploader/compose"
	s3uploaderbuilder "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/uploader/s3"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkguploader "github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// Newはretryable/permanentの同名fileが同じobject keyへ衝突しないよう、failure categoryを
// providerのprefixへ反映してから設定済みtargetのuploaderを組み立てます。この変換は
// destination identityの生成前に必要なため、inline writerとmaintenance runnerで共有します。
func New(
	cfg objectstorage.UploadTargetConfig,
	failureCategory fsfailurearchive.FailureCategory,
) (pkguploader.Uploader, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	return compose.FromTargets(
		objectstorage.NewUploadTargetConfigAdapter(uploadTargetConfigForFailureCategory(cfg, failureCategory)),
		s3uploaderbuilder.Builders(),
	)
}

// uploadTargetConfigForFailureCategoryは呼び出し元の設定を変更せず、failure archive分類を
// object storage上のnamespaceへ反映した設定copyを返します。
func uploadTargetConfigForFailureCategory(
	cfg objectstorage.UploadTargetConfig,
	failureCategory fsfailurearchive.FailureCategory,
) objectstorage.UploadTargetConfig {
	targetCfg := cfg
	targetCfg.S3.Prefix = ObjectStoragePrefix(targetCfg.S3.Prefix, failureCategory)
	targetCfg.S3Destinations = slices.Clone(cfg.S3Destinations)
	for index := range targetCfg.S3Destinations {
		targetCfg.S3Destinations[index].Prefix = ObjectStoragePrefix(targetCfg.S3Destinations[index].Prefix, failureCategory)
	}
	return targetCfg
}
