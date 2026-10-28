package outputsettings

import (
	"context"
	"fmt"
	"slices"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// 監査logのfieldの名前です。どの記録先でも同じ名前で記録し、形式は各記録先の作法に
// 従います。要求の識別子は、pkg/loggerのrequest IDとして記録します。
var (
	auditKindKey        = pkglogger.MustFieldKey("log_kind")
	auditPhaseKey       = pkglogger.MustFieldKey("audit_phase")
	auditRemoteAddrKey  = pkglogger.MustFieldKey("remote_addr")
	auditReasonKey      = pkglogger.MustFieldKey("reason")
	auditChangesKey     = pkglogger.MustFieldKey("changes")
	auditSettingsKey    = pkglogger.MustFieldKey("settings")
	auditVerifiedKey    = pkglogger.MustFieldKey("verified")
	auditLoggerKey      = pkglogger.MustFieldKey("logger")
	auditOutputKey      = pkglogger.MustFieldKey("output")
	auditExpectedKey    = pkglogger.MustFieldKey("expected")
	auditActualKey      = pkglogger.MustFieldKey("actual")
	auditUndeliveredKey = pkglogger.MustFieldKey("undelivered_phases")
)

// auditKindは、標準出力などで通常のlogと同じ流れに混ざっても、受け取る側で監査logを
// 振り分けられるように、すべての記録へ付ける印です。
const auditKind = "audit"

// 監査logの段階です。一つの要求の記録は、要求IDで結びます。
const (
	auditPhaseRequested   = "requested"
	auditPhaseRejected    = "rejected"
	auditPhaseStarted     = "started"
	auditPhaseApplied     = "applied"
	auditPhaseMismatch    = "mismatch"
	auditPhaseFailed      = "failed"
	auditPhaseUndelivered = "undelivered"
)

// auditTrailは、一つの要求の監査logを記録し、記録先のすべてへ届かなかった記録の段階を
// 覚えます。届かなくても処理は止めず、finishでそのことを記録します。
type auditTrail struct {
	logger      pkglogger.DeliveryLogger
	request     Request
	undelivered []string
}

func (t *auditTrail) info(ctx context.Context, phase, message string, fields ...pkglogger.Field) {
	report, err := t.logger.Info(ctx, message, t.fields(phase, fields)...)
	t.delivered(phase, report, err)
}

func (t *auditTrail) warn(ctx context.Context, phase, message string, fields ...pkglogger.Field) {
	report, err := t.logger.Warn(ctx, message, t.fields(phase, fields)...)
	t.delivered(phase, report, err)
}

func (t *auditTrail) error(ctx context.Context, phase, message string, fields ...pkglogger.Field) {
	report, err := t.logger.Error(ctx, message, t.fields(phase, fields)...)
	t.delivered(phase, report, err)
}

func (t *auditTrail) fields(phase string, extra []pkglogger.Field) []pkglogger.Field {
	return append([]pkglogger.Field{
		auditKindKey.String(auditKind),
		auditPhaseKey.String(phase),
		pkglogger.WithRequestID(t.request.ID),
		auditRemoteAddrKey.String(t.request.RemoteAddr),
	}, extra...)
}

// deliveredは、記録が記録先のすべてへ届いたかを、送達確認loggerのerrorで確かめます。
// 送達確認loggerは、失敗した記録先や結果を確認できない記録先があればerrorを返し、
// levelの設定で対象外の記録先（skipped）と、batchで送るClickHouseが受理した記録
// （enqueued）はerrorにしません。HasUnregisteredTargetは対象外の記録先も未登録と
// 数えるため、ERRORだけを受け取るSlackを記録先に含めると、INFOの記録がすべて届かな
// かったことになってしまいます。失敗時の保存先に控えが残っても、届いたことにはなりません。
func (t *auditTrail) delivered(phase string, _ pkglogger.DeliveryReport, err error) {
	if err != nil {
		t.undelivered = append(t.undelivered, phase)
	}
}

// finishは、記録先のすべてへ届かなかった記録があれば、その段階をERRORで記録し、trueを
// 返します。この記録も届かないことがあるため、結果は呼び出し元へも返します。
func (t *auditTrail) finish(ctx context.Context) bool {
	if len(t.undelivered) == 0 {
		return false
	}
	phases := slices.Clone(t.undelivered)
	t.error(ctx, auditPhaseUndelivered, "admin API audit records were not delivered to every audit output",
		anyField(auditUndeliveredKey, phases))
	return true
}

// anyFieldは、JSONで表せる値のfieldを返します。監査logの値は文字列・数値・真偽値の
// mapとsliceだけなので失敗しませんが、失敗した場合も記録を欠かさないよう、文字列に
// して残します。
func anyField(key pkglogger.FieldKey, value any) pkglogger.Field {
	field, err := key.Any(value)
	if err != nil {
		return key.String(fmt.Sprint(value))
	}
	return field
}

// settingsValueは、出力先の設定を、管理用APIの文書と同じ項目名の値にします。
func settingsValue(settings pkglogger.OutputSettings) map[string]any {
	s := settings.Sampling
	return map[string]any{
		"level": settings.Level.String(),
		"sampling": map[string]any{
			"enabled":         s.IsSamplingEnabled,
			"by_request_id":   s.EnableLoggingByRequestID,
			"request_id_rate": s.LoggingByRequestIDRate,
			"debug_rate":      s.SamplingDebugRate,
			"info_rate":       s.SamplingInfoRate,
			"warn_rate":       s.SamplingWarnRate,
			"error_rate":      s.SamplingErrorRate,
		},
	}
}

// changeValueは、変更で指定した項目だけを、管理用APIの文書と同じ項目名の値にします。
func changeValue(change pkglogger.OutputSettingsChange) map[string]any {
	value := map[string]any{}
	if change.Level != nil {
		value["level"] = change.Level.String()
	}
	if s := change.Sampling; s != nil {
		sampling := map[string]any{}
		setValue(sampling, "enabled", s.IsSamplingEnabled)
		setValue(sampling, "by_request_id", s.EnableLoggingByRequestID)
		setValue(sampling, "request_id_rate", s.LoggingByRequestIDRate)
		setValue(sampling, "debug_rate", s.SamplingDebugRate)
		setValue(sampling, "info_rate", s.SamplingInfoRate)
		setValue(sampling, "warn_rate", s.SamplingWarnRate)
		setValue(sampling, "error_rate", s.SamplingErrorRate)
		value["sampling"] = sampling
	}
	return value
}

func setValue[T any](target map[string]any, name string, value *T) {
	if value != nil {
		target[name] = *value
	}
}

// changesValueは、要求した全loggerの変更を、loggerの名前と出力先ごとの値にします。
func changesValue(changes map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) map[string]any {
	value := make(map[string]any, len(changes))
	for name, outputs := range changes {
		changed := make(map[string]any, len(outputs))
		for output, change := range outputs {
			changed[string(output)] = changeValue(change)
		}
		value[name] = changed
	}
	return value
}
