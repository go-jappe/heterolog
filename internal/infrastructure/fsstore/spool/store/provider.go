package store

type SpoolStoreConfigProvider interface {
	// SpoolPathはspool rootを組み立てるbase directoryです。例: /var/lib
	SpoolPath() string
	// TargetはSpoolPath直下へ配置するapplication識別用の単一path要素です。例: heterolog
	Target() string
	// Dirは<SpoolPath>/<Target>/spool直下へ配置する、一つのwriterとrunnerが専有する
	// spool識別用の単一path要素です。相対pathではないため、値に含まれるslashは
	// directory階層として扱わず安全な文字へ置換します。独立したpipelineを区別できる
	// 名前を指定し、現在の既定値ではsync OpenSearchにopensearch、async OpenSearchに
	// opensearch_asyncを使用します。例えばDirがopensearch_asyncならrootは
	// /var/lib/heterolog/spool/opensearch_asyncとなり、その直下にactive、processing、
	// done、dead、quarantineを配置します。
	Dir() string
}
