package logconfig

// InternalLogDestinationConfigは、loggerの内部処理を記録するlogを通常の
// application logとは異なる保存先へ委譲する場合の、backendごとの保存先identity上書き値です。
//
// string fieldは空文字、LokiLabelsはnilの場合に通常backend設定を継承します。
// FileSeparateDebugLogDirEnabledはnilなら通常backendのdirectory分離設定を継承し、
// false/trueなら明示的に上書きします。falseの場合FileDebugLogDirは無視します。
// trueの場合はFileDebugLogDirが空なら通常backendのraw DebugLogDirを継承し、
// 継承後も空白だけなら設定検証でエラーにします。pathの有無で分離を切り替えません。
// file path、index、label、streamなどの保存先を識別する値だけを保持し、
// timeout、retry、認証などの送信方法は保持しません。
// この設定だけでbackendを起動することはなく、各HandlerConfigで委譲先に選ばれた
// backendに対してだけ上書きを適用します。
// この設定はappendlog lifecycleだけでなく、backend送信、failure store、spoolなど、
// 同じLoggerContractKindで発生するすべてのinternal logに適用されます。
// file directoryは継承・上書き後のpathで共有規則を検証します。error/debugを両方
// 分離する場合、internal logのdefaultとは別のdirectoryをerror/debugだけで共有することはできません。
type InternalLogDestinationConfig struct {
	FileLogPath                    string
	FileTarget                     string
	FileLogDir                     string
	FileErrorLogDir                string
	FileDebugLogDir                string
	FileSeparateDebugLogDirEnabled *bool

	// SlackChannelは共通channelだけを上書きします。未指定のレベル別channelは
	// 通常backendから継承し、空でなければ共通channelより優先されます。
	// 全レベルを別の宛先へ変更するには、継承されるレベル別channelも上書きしてください。
	SlackChannel      string
	SlackErrorChannel string
	SlackWarnChannel  string
	SlackInfoChannel  string
	SlackDebugChannel string

	OpenSearchIndex string
	// LokiLabelsはnilなら通常backendの追加labelを継承します。非nilならmap全体を
	// 置き換え、空mapは追加labelを消去します。通常backendのmapとのmergeは行いません。
	// appName・loglevel・envはloggerが生成するため、指定しても上書きされません。
	// 不正なlabel名は、この保存先を使用する場合に起動時の設定検証でエラーにします。
	LokiLabels map[string]string

	CloudWatchLogsLogGroupName  string
	CloudWatchLogsLogStreamName string
	ClickHouseDatabase          string
	ClickHouseTable             string
}
