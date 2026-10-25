package config

import (
	"strings"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// loadRetryClientModeは読み込み済みのretry client modeを解析します。
//
// rawが空文字または空白だけの場合はdefaultModeを返します。明示値は正規化した上で
// 検証し、対応していないmodeをdefaultへ置き換えず、元の値を特定できるerrorを返します。
// keyはエラー表示にだけ使います。
func loadRetryClientMode(
	raw, key string,
	defaultMode pkglogger.RetryClientMode,
) (pkglogger.RetryClientMode, error) {
	if strings.TrimSpace(raw) == "" {
		return defaultMode, nil
	}

	mode := pkglogger.ParseRetryClientMode(raw, defaultMode)
	if !mode.IsValid() {
		return mode, newInvalidEnvironmentValueError(
			key, raw, ErrUnsupportedRetryClientMode,
			"environment variable %q has unsupported retry client mode: %q",
			key,
			raw,
		)
	}
	return mode, nil
}
