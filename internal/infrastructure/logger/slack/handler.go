package slack

import (
	"context"
	"fmt"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/slack/errors"
)

// スラック送信エラー時のハンドリング
func (s *SlackLogger) handleSlackError(ctx context.Context, err error) {
	entry := s.InternalErrorEntryWithStack("Slack送信エラー", "", err, logging.GetCallerDepth(NewStackTraceConfigAdapter(s.config)))
	wrapped := &errors.SlackLoggerInternalError{Op: MsgDispachError, Err: err}
	handler.CallErrorHandlerIfPresent(s.OnError, ctx, wrapped, entry, s.Tracer, s.Meter)
}

// スラック送信成功時のハンドリング
func (s *SlackLogger) handleSlackSuccess(ctx context.Context, ch, ts string) {
	mes := fmt.Sprintf("Slackメッセージ送信成功 - チャンネル: %s, タイムスタンプ: %s", ch, ts)
	handler.CallInfoHandlerIfPresent(s.OnInfo, ctx, s.InternalInfoEntry(mes, ""), s.Tracer, s.Meter)
}
