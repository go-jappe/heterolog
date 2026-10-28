package outputsettings

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

var (
	// ErrNoLoggersは、Serviceに管理するloggerが一つもないことを表します。
	ErrNoLoggers = errors.New("no loggers to manage")
	// ErrNoAuditLoggerは、Serviceに監査logの記録先がないことを表します。
	ErrNoAuditLogger = errors.New("no audit logger for the admin API")
	// ErrUnknownLoggerは、Serviceにない名前のloggerへの変更であることを表します。
	ErrUnknownLogger = errors.New("unknown logger")
	// ErrApplyFailedは、検証を通った変更を反映できなかったことを表します。検証の後で
	// 別の変更と競合した場合などに生じ、それより前のloggerの変更は反映済みです。
	ErrApplyFailed = errors.New("output settings change could not be applied")
	// ErrSharedLiveSettingsは、Serviceで扱う出力先のうち二つが、同じLiveSettingsを共有する
	// ことを表します。一方への変更がもう一方も変え、その変化を監査logに記録することも、
	// 要求と照合することもできないため、Serviceを作りません。
	ErrSharedLiveSettings = errors.New("outputs managed by the admin API share live settings")
)

// LiveSettingsOwnerは、出力先ごとのLiveSettingsを示せるloggerです。heterologが作る
// loggerの内部のruntimeが実装し、NewServiceは、これで出力先どうしの共有を見つけます。
type LiveSettingsOwner interface {
	OutputLiveSettings() map[pkglogger.LogOutputDestination]*pkglogger.LiveSettings
}

// Serviceは、名前を付けた複数のloggerの出力先の設定を、まとめて読み取り・変更します。
// 管理用APIの処理です。変更は一つずつ順に処理し、要求、変更を始める時点の設定、変更後の
// 設定を監査logへ記録して、変更後の設定が要求どおりかを確かめます。
type Service struct {
	loggers map[string]pkglogger.SettingsController
	audit   pkglogger.DeliveryLogger
	// muは変更を一つずつ順に処理します。後から来た変更は、前の変更の記録を終えてから
	// 始めます。
	mu sync.Mutex
}

// NewServiceは、loggersを扱い、監査logをauditへ記録するServiceを返します。loggersは
// 複製して保持します。loggerが一つもない場合はErrNoLoggers、auditがnilの場合は
// ErrNoAuditLoggerを返します。LiveSettingsOwnerを実装するloggerの出力先どうしが、同じ
// LiveSettingsを共有している場合はErrSharedLiveSettingsを返します。同じloggerを二つの
// 名前で渡した場合も共有です。
func NewService(loggers map[string]pkglogger.SettingsController, audit pkglogger.DeliveryLogger) (*Service, error) {
	if len(loggers) == 0 {
		return nil, ErrNoLoggers
	}
	if audit == nil {
		return nil, ErrNoAuditLogger
	}
	if err := checkSharedLiveSettings(loggers); err != nil {
		return nil, err
	}
	return &Service{loggers: maps.Clone(loggers), audit: audit}, nil
}

// checkSharedLiveSettingsは、LiveSettingsOwnerを実装するloggerの出力先どうしで、同じ
// LiveSettingsを共有していないかを確かめ、共有している組ごとのerrorを返します。実装
// しないloggerは確かめられないため、対象外です。
func checkSharedLiveSettings(loggers map[string]pkglogger.SettingsController) error {
	type owner struct {
		logger string
		output pkglogger.LogOutputDestination
	}
	owners := map[*pkglogger.LiveSettings]owner{}
	var errs []error
	// 毎回同じ順で確かめ、errorを同じ順で報告します。
	for _, name := range slices.Sorted(maps.Keys(loggers)) {
		source, ok := loggers[name].(LiveSettingsOwner)
		if !ok {
			continue
		}
		outputs := source.OutputLiveSettings()
		for _, output := range slices.Sorted(maps.Keys(outputs)) {
			live := outputs[output]
			if live == nil {
				continue
			}
			if first, shared := owners[live]; shared {
				errs = append(errs, fmt.Errorf("%w: logger %q output %q and logger %q output %q",
					ErrSharedLiveSettings, first.logger, first.output, name, output))
				continue
			}
			owners[live] = owner{logger: name, output: output}
		}
	}
	return errors.Join(errs...)
}

// Requestは、一つの要求を監査logで識別する情報です。
type Request struct {
	// IDは要求ごとの識別子で、同じ要求の監査logを結びます。
	ID string
	// RemoteAddrは、要求の送り元のaddressです。
	RemoteAddr string
}

// Mismatchは、変更後の設定が、変更を始める時点の設定に要求を適用した設定と違った
// 出力先です。設定は戻しません。
type Mismatch struct {
	Logger   string
	Output   pkglogger.LogOutputDestination
	Expected pkglogger.OutputSettings
	Actual   pkglogger.OutputSettings
}

// Resultは、Updateの結果です。
type Result struct {
	// Mismatchesは、変更後の設定が要求どおりでなかった出力先を、loggerの名前と出力先の
	// 順に持ちます。
	Mismatches []Mismatch
	// AuditUndeliveredは、この要求の監査logの一部が、記録先のすべてへ届かなかったことを
	// 表します。変更は止めません。
	AuditUndelivered bool
}

// Settingsは、loggerの名前ごとに、各出力先の現在の設定を返します。
func (s *Service) Settings() map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettings {
	result := make(map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettings, len(s.loggers))
	for name, logger := range s.loggers {
		result[name] = logger.OutputSettings()
	}
	return result
}

// Rejectは、設定を変える前に拒否した要求を、reasonとともに監査logへ記録します。記録が
// 記録先のすべてへ届かなかった場合は、そのこともERRORで記録し、trueを返します。
func (s *Service) Reject(ctx context.Context, request Request, reason string) bool {
	// 送り元が切断しても、監査logは最後まで記録します。
	ctx = context.WithoutCancel(ctx)
	trail := &auditTrail{logger: s.audit, request: request}
	trail.warn(ctx, auditPhaseRejected, "admin API request rejected", auditReasonKey.String(reason))
	return trail.finish(ctx)
}

// Updateは、変更を一つずつ順に処理します。要求を監査logへ記録してから全loggerの全変更を
// 検証し、変更を始める時点の設定を記録して反映し、変更後の設定を記録します。変更後の
// 設定が、始める時点の設定に要求を適用した設定と違えば、その出力先をResultに含めて
// ERRORで記録し、設定は戻しません。監査logが記録先のすべてへ届かなくても変更は続け、
// ResultとERRORの記録で知らせます。
//
// 一つでも不正な変更があれば、どれも反映せず、拒否として記録します。Serviceにない
// loggerはErrUnknownLogger、loggerが使っていない出力先はErrUnknownOutput、不正な値は
// pkg/loggerのConfigFieldErrorです。検証の後で反映に失敗した場合は、それまでに反映した
// 変更を記録し、ErrApplyFailedを包んだerrorを返します。
func (s *Service) Update(ctx context.Context, request Request, changes map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 送り元が切断しても、監査logは最後まで記録します。
	ctx = context.WithoutCancel(ctx)
	trail := &auditTrail{logger: s.audit, request: request}
	trail.info(ctx, auditPhaseRequested, "admin API settings change requested", anyField(auditChangesKey, changesValue(changes)))

	plans, err := s.plan(changes)
	if err != nil {
		trail.warn(ctx, auditPhaseRejected, "admin API request rejected", auditReasonKey.String(err.Error()))
		return Result{AuditUndelivered: trail.finish(ctx)}, err
	}
	trail.info(ctx, auditPhaseStarted, "admin API settings change started", anyField(auditSettingsKey, startValue(plans)))

	var result Result
	applied := map[string]map[pkglogger.LogOutputDestination]pkglogger.AppliedOutputSettings{}
	var applyErr error
	for _, p := range plans {
		outputs, err := p.logger.UpdateOutputSettings(p.changes)
		if len(outputs) > 0 {
			applied[p.name] = outputs
		}
		if err != nil {
			applyErr = fmt.Errorf("%w: logger %q: %w", ErrApplyFailed, p.name, err)
			break
		}
		for _, output := range slices.Sorted(maps.Keys(p.changes)) {
			expected := p.expected[output]
			actual, ok := outputs[output]
			if !ok || actual.After != expected {
				result.Mismatches = append(result.Mismatches, Mismatch{Logger: p.name, Output: output, Expected: expected, Actual: actual.After})
			}
		}
	}
	if applyErr != nil {
		trail.error(ctx, auditPhaseFailed, "admin API settings change failed",
			auditReasonKey.String(applyErr.Error()), anyField(auditSettingsKey, appliedValue(applied)))
		result.AuditUndelivered = trail.finish(ctx)
		return result, applyErr
	}
	trail.info(ctx, auditPhaseApplied, "admin API settings change applied",
		anyField(auditSettingsKey, appliedValue(applied)), auditVerifiedKey.Bool(len(result.Mismatches) == 0))
	for _, m := range result.Mismatches {
		trail.error(ctx, auditPhaseMismatch, "admin API settings change does not match the request",
			auditLoggerKey.String(m.Logger), auditOutputKey.String(string(m.Output)),
			anyField(auditExpectedKey, settingsValue(m.Expected)), anyField(auditActualKey, settingsValue(m.Actual)))
	}
	result.AuditUndelivered = trail.finish(ctx)
	return result, nil
}

// planは、一つのloggerへの変更と、変更を始める時点のそのloggerの設定、変更後に期待する
// 設定です。
type plan struct {
	name     string
	logger   pkglogger.SettingsController
	start    map[pkglogger.LogOutputDestination]pkglogger.OutputSettings
	changes  map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange
	expected map[pkglogger.LogOutputDestination]pkglogger.OutputSettings
}

// planは、全loggerの全変更を今の設定に適用して検証し、loggerの名前の順に反映の計画を
// 返します。
func (s *Service) plan(changes map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) ([]plan, error) {
	var plans []plan
	for _, name := range slices.Sorted(maps.Keys(changes)) {
		logger, ok := s.loggers[name]
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrUnknownLogger, name)
		}
		start := logger.OutputSettings()
		expected, err := expect(name, start, changes[name])
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan{name: name, logger: logger, start: start, changes: changes[name], expected: expected})
	}
	return plans, nil
}

// expectは、startの設定を写したLiveSettingsへchangesを当てて、変更後に期待する設定を
// 出力先ごとに返します。反映と同じpkg/loggerのUpdateLiveSettingsを使うため、項目の当て方と
// 検証は反映と一致します。startにない出力先はErrUnknownOutput、不正な値はpkg/loggerの
// ConfigFieldErrorです。
func expect(
	name string,
	start map[pkglogger.LogOutputDestination]pkglogger.OutputSettings,
	changes map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange,
) (map[pkglogger.LogOutputDestination]pkglogger.OutputSettings, error) {
	outputs := make(map[pkglogger.LogOutputDestination]*pkglogger.LiveSettings, len(changes))
	for _, output := range slices.Sorted(maps.Keys(changes)) {
		current, ok := start[output]
		if !ok {
			return nil, fmt.Errorf("logger %q: %w: %q", name, pkglogger.ErrUnknownOutput, output)
		}
		live, err := pkglogger.NewLiveSettings(current.Level, current.Sampling)
		if err != nil {
			return nil, err
		}
		outputs[output] = live
	}
	applied, err := pkglogger.UpdateLiveSettings(name, outputs, changes)
	if err != nil {
		return nil, err
	}
	expected := make(map[pkglogger.LogOutputDestination]pkglogger.OutputSettings, len(applied))
	for output, a := range applied {
		expected[output] = a.After
	}
	return expected, nil
}

// startValueは、変更する出力先の、変更を始める時点の設定を監査logの値にします。
func startValue(plans []plan) map[string]any {
	value := make(map[string]any, len(plans))
	for _, p := range plans {
		outputs := make(map[string]any, len(p.changes))
		for output := range p.changes {
			outputs[string(output)] = settingsValue(p.start[output])
		}
		value[p.name] = outputs
	}
	return value
}

// appliedValueは、反映した出力先の、変更後の設定を監査logの値にします。
func appliedValue(applied map[string]map[pkglogger.LogOutputDestination]pkglogger.AppliedOutputSettings) map[string]any {
	value := make(map[string]any, len(applied))
	for name, outputs := range applied {
		settings := make(map[string]any, len(outputs))
		for output, a := range outputs {
			settings[string(output)] = settingsValue(a.After)
		}
		value[name] = settings
	}
	return value
}
