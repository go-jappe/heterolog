package worker

type ConfigProvider interface {
	// SpoolPathはspool rootを組み立てるbase directoryです。例: /var/lib
	SpoolPath() string
	// TargetはSpoolPath直下へ配置するapplication識別用の単一path要素です。例: heterolog
	Target() string
	// Dirは<SpoolPath>/<Target>/spool直下へ配置する、一つのwriterとrunnerが専有する
	// spool識別用の単一path要素です。相対pathではないため、値に含まれるslashは
	// directory階層として扱いません。例: opensearch、opensearch_async。
	Dir() string
}
