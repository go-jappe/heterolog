package slack

import (
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/internal/application/logging/logtemplate"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
)

// stopBatchTimerLockedはmuの下で予約を失効させます。発火済みcallbackは
// 自身でtimerWGを完了させ、送信前の所有権確認で古い予約を無視します。
func (s *SlackLogger) stopBatchTimerLocked() {
	if s.timer == nil {
		return
	}
	if s.timer.Stop() {
		s.timerWG.Done()
	}
	s.timer = nil
}

type slackBatchSchedule struct {
	timer *time.Timer // mu protects initialization and ownership checks.
}

// scheduleBatchTimerLockedはmuを保持して呼びます。sendはsendMuの下で実行し、
// bufferの取り出しからHTTP送信までを他のcallbackやCloseと直列化します。
func (s *SlackLogger) scheduleBatchTimerLocked(delay time.Duration, send func([]*logging.LogEntry)) {
	schedule := &slackBatchSchedule{}
	s.timerWG.Add(1)
	schedule.timer = time.AfterFunc(delay, func() {
		defer s.timerWG.Done()
		// timerの代入はmuの解放より前に完了するため、delay=0でも競合しません。
		s.runScheduledBatch(schedule, send)
	})
	s.timer = schedule.timer
}

func (s *SlackLogger) runScheduledBatch(schedule *slackBatchSchedule, send func([]*logging.LogEntry)) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	s.mu.Lock()
	if s.timer != schedule.timer {
		s.mu.Unlock()
		return
	}
	s.timer = nil
	logs := s.takeBufferedLogsLocked()
	s.mu.Unlock()
	send(logs)
}

// takeBufferedLogsLockedはmuの下でbatchと次の通知間隔を同時に確定します。
// 空bufferや失効済みcallbackは通知間隔を進めません。
func (s *SlackLogger) takeBufferedLogsLocked() []*logging.LogEntry {
	if len(s.logs) == 0 {
		return nil
	}
	logs := append([]*logging.LogEntry(nil), s.logs...)
	clear(s.logs)
	s.logs = s.logs[:0]
	s.intervalIndex, s.nextNotificationImmediate = nextSlackNotificationState(
		s.config.Intervals(), s.intervalIndex, s.nextNotificationImmediate,
	)
	return logs
}

// slackNotificationDelayは現在の送信状態の待機時間と、有効範囲へ
// 補正したindexを返します。immediateは設定値ではなくloggerの状態です。
// intervalsはconstructorで検証済みのため、1件以上の正の値を前提とします。
func slackNotificationDelay(
	intervals []time.Duration,
	index int,
	immediate bool,
) (time.Duration, int) {
	if index < 0 {
		index = 0
	}
	if index >= len(intervals) {
		index = len(intervals) - 1
	}
	if immediate {
		return 0, index
	}
	return intervals[index], index
}

// nextSlackNotificationStateは送信対象batchを取り出した後の状態を返します。
// 初回の即時送信後はIntervals[0]へ移り、以降は末尾まで進みます。
func nextSlackNotificationState(
	intervals []time.Duration,
	current int,
	immediate bool,
) (int, bool) {
	if current < 0 {
		current = 0
	}
	if current >= len(intervals) {
		current = len(intervals) - 1
	}
	if immediate {
		return current, false
	}
	if current >= len(intervals)-1 {
		return len(intervals) - 1, false
	}
	return current + 1, false
}

type slackChannelBatch struct {
	channel  string
	messages []string
}

// slackChannelBatchesはentryごとのlevelから送信channelを決め、同じchannelだけを
// 一つの投稿へまとめます。channelの投稿順とchannel内のentry順はいずれも新しい順です。
func slackChannelBatches(
	config SlackLoggerConfigProvider,
	logs []*logging.LogEntry,
	logFormatter formatter.PlainTextFormatter,
) []slackChannelBatch {
	batches := make([]slackChannelBatch, 0, len(logs))
	indexByChannel := make(map[string]int)
	for i := len(logs) - 1; i >= 0; i-- {
		entry := logs[i]
		if entry == nil {
			continue
		}

		channel := channelForLogLevel(config, entry.Level)
		batchIndex, exists := indexByChannel[channel]
		if !exists {
			batchIndex = len(batches)
			indexByChannel[channel] = batchIndex
			batches = append(batches, slackChannelBatch{channel: channel})
		}
		batches[batchIndex].messages = append(
			batches[batchIndex].messages,
			fmt.Sprintf(
				logtemplate.ErrMessageSlack,
				logFormatter.Format(entry),
				config.Runtime().AppName(),
			),
		)
	}
	return batches
}
