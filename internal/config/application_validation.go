package config

import "errors"

// ValidateApplicationConfig は、読み込みとruntime overrideが完了したAppConfigについて、
// 複数のlogger、backend、storeを組み合わせたときのアプリケーション全体の整合性を
// 検証します。
//
// 個々の環境変数を正しく読み込めたかはLoadConfig、各backendへ渡す設定の妥当性は
// mapperと受け取り先packageの責務です。この関数では、それら単独では判断できない
// runtime health policy、failure store catalogの配線、timeout階層、storage directory所有権、
// file lifecycleとupload対象の組合せを扱います。
func ValidateApplicationConfig(a AppConfig) error {
	return errors.Join(
		validateRuntimeMemoryHealthConfig(a.RuntimeConfig.Health.Memory),
		validateFailureStoreCatalog(a),
		validateLoggerTimeoutHierarchy(a),
		validateStorageDirectoryOwnership(a),
		validateLogFileObjectStorageUploadActivation(a),
	)
}
