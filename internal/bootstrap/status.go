package bootstrap

import (
	appstatus "github.com/go-jappe/heterolog/internal/application/status"
	infrastatus "github.com/go-jappe/heterolog/internal/infrastructure/status"
)

// StatusProbesは、管理用APIのstatusの報告が使う、このOSの読み取り（CPU・memory・
// directoryのscan・diskの空き）を返します。
func StatusProbes() appstatus.Probes { return infrastatus.Probes() }
