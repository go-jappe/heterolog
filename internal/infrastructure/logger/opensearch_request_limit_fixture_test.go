package logger

const (
	// Test用の明示値です。direct constructorは0を未指定として補完せず、不正値として
	// 拒否します。productionのdefault適用はinternal/configのenvironment loaderが担います。
	testOpenSearchBufferFlushBytes    = 5 * 1024 * 1024
	testOpenSearchMaxBulkRequestBytes = 15 * 1024 * 1024
)
