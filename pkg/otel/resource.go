package otel

// Resource は OpenTelemetry におけるリソース属性を表します。
// リソースは「どの実行主体からテレメトリが送られたか」を識別するための
// 不変メタデータ（プロセス単位・サービス単位）です。
type Resource struct {
	// ServiceName はサービスの論理名を表します。
	// 例: "payment-api", "heterolog-batch"
	// OpenTelemetry の service.name に対応します。
	ServiceName string

	// ServiceVersion はデプロイされているサービスのバージョンを表します。
	// 例: "1.2.3", "2026-02-25"
	// デプロイ差分や不具合調査時の識別に使用されます。
	ServiceVersion string

	// CloudProvider はクラウドプロバイダ名を表します。
	// 例: "aws", "gcp", "azure", "on-prem"
	// クラウド環境ごとの可視化や分析で使用されます。
	CloudProvider string

	// HostName は実行ホスト名を表します。
	// 例: EC2のインスタンス名、コンテナホスト名など。
	// ホスト単位の障害分析や負荷分析に使用されます。
	HostName string

	// OSType は実行環境のOS種別を表します。
	// 例: "linux", "darwin", "windows"
	// 環境依存問題の分析に使用されます。
	OSType string

	// Environment は実行環境の区分を表します。
	// 例: "production", "staging", "development"
	// 環境別のフィルタリングや分離に使用されます。
	Environment string
}
