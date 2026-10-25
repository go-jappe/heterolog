package app

const (
	// アプリ名
	DefaultAppName = "heterolog"
	// アプリ内部名
	DefaultTarget        = "heterolog"
	AppVersion    string = "dev-0.0.1"
)

type AppConfig struct {
	AppName    string // appName
	AppVersion string
}
