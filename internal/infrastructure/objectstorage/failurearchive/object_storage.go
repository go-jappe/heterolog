package failurearchive

import (
	"strings"

	fsfailurearchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
)

// ObjectStoragePrefixは、failure archive共通のprefixへrecord分類を付与します。
// retryableとpermanentには同じfile名が存在し得るため、object keyの名前空間を
// 分離して一方のuploadがもう一方を上書きしないようにします。
func ObjectStoragePrefix(basePrefix string, failureCategory fsfailurearchive.FailureCategory) string {
	basePrefix = strings.Trim(basePrefix, "/")
	failureCategorySegment := strings.Trim(string(failureCategory), "/")
	if basePrefix == "" {
		return failureCategorySegment
	}
	if failureCategorySegment == "" {
		return basePrefix
	}
	return basePrefix + "/" + failureCategorySegment
}
