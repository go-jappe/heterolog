package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	_ "time/tzdata"

	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/config"
	cli "github.com/go-jappe/heterolog/internal/presentation/cli/logrecovery"
)

func main() {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	control := cli.NewSignalControl(context.Background(), signals)
	exitCode := cli.Run(control.Context(), os.Args[1:], os.Stdout, os.Stderr, cli.Dependencies{
		LoadConfig: loadConfig,
		Recover:    bootstrap.RecoverLogs,
		ForceStop:  control.ForceStop(),
	})
	signal.Stop(signals)
	control.Close()
	os.Exit(exitCode)
}

// loadConfigは、--env-fileで指定したfile、省略時はHETEROLOG_ENV_PATHまたは.envから設定を
// 読み込みます。process environmentは変更しません。
func loadConfig(envFile string) (config.AppConfig, error) {
	if envFile != "" {
		return config.LoadConfigFile(envFile)
	}
	return config.LoadConfig()
}
