package config

import pkgenv "github.com/go-jappe/heterolog/pkg/env"

// AdminServerConfigは、稼働中に出力先の設定を変更する管理用APIの、管理用サーバーの
// 環境設定です。Addrが空なら管理用サーバーを起動しません。値の検証と起動は、
// 公開APIのLoadEnvConfigとStartAdminServerが行います。Tokenは秘密のため、表示しないでください。
type AdminServerConfig struct {
	Addr        string
	Token       string
	TLSCertFile string
	TLSKeyFile  string
}

func loadAdminServerConfig(env pkgenv.Lookup) AdminServerConfig {
	return AdminServerConfig{
		Addr:        env.GetEnvString("HETEROLOG_ADMIN_ADDR", ""),
		Token:       env.GetEnvString("HETEROLOG_ADMIN_TOKEN", ""),
		TLSCertFile: env.GetEnvString("HETEROLOG_ADMIN_TLS_CERT_FILE", ""),
		TLSKeyFile:  env.GetEnvString("HETEROLOG_ADMIN_TLS_KEY_FILE", ""),
	}
}
