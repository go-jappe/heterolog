// Package telemetryは、loggerが記録するmetricsの名前と属性をまとめます。
// 名前は利用者のdashboardやalertが参照するため、変えたらdocs/reference.mdと
// docs/reference.ja.mdの一覧も直します。
package telemetry

// metricsの名前です。Prometheusの慣習に合わせ、単位を名前の末尾に付けます。
const (
	// FileDroppedEntriesは、file loggerがoverloadの方針dropで捨てたentryの数です。
	FileDroppedEntries = "heterolog_file_dropped_entries_total"
)
