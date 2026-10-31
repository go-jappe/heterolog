// Package adminは、稼働中のloggerの出力先ごとのlog levelとsampling設定を読み取り・
// 変更し、loggerとtelemetryを作った設定を返す、HTTPの管理用APIの受け口です。要求の
// 認証、経路、JSONの変換、errorと状態codeの対応を扱い、設定の検証と反映はapplication層
// のoutputsettingsに、設定の文書はconfigviewに任せます。
package admin

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/go-jappe/heterolog/internal/application/configview"
	"github.com/go-jappe/heterolog/internal/application/outputsettings"
	"github.com/go-jappe/heterolog/internal/application/status"
	pkgadmin "github.com/go-jappe/heterolog/pkg/admin"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/google/uuid"
)

// maxRequestBytesは、管理用APIの要求本文の上限です。
const maxRequestBytes = 64 << 10

var (
	// ErrTokenTooShortは、tokenがMinTokenLengthより短いことを表します。tokenは含めません。
	ErrTokenTooShort = fmt.Errorf("admin token must be at least %d bytes", pkgadmin.MinTokenLength)
	// ErrDuplicateNameは、Membersの二つのmapに同じ名前があることを表します。
	ErrDuplicateName = errors.New("admin API member name is used more than once")
	// ErrInvalidStatusConfigは、StatusConfigの値が範囲外（負の間隔、0から100でない
	// percent）であることを表します。
	ErrInvalidStatusConfig = errors.New("invalid admin API status config")
)

// ValidateStatusConfigは、StatusConfigの値の範囲を確かめます。0は既定値を使うため、
// 負の値と100を超えるpercentだけを拒否します。
func ValidateStatusConfig(cfg pkgadmin.StatusConfig) error {
	var errs []error
	if cfg.DirectoryScanInterval < 0 {
		errs = append(errs, fmt.Errorf("%w: DirectoryScanInterval must not be negative", ErrInvalidStatusConfig))
	}
	if cfg.SlowRequest < 0 {
		errs = append(errs, fmt.Errorf("%w: SlowRequest must not be negative", ErrInvalidStatusConfig))
	}
	for name, percent := range map[string]float64{
		"DiskFreeWarningPercent": cfg.DiskFreeWarningPercent, "DiskFreeCriticalPercent": cfg.DiskFreeCriticalPercent,
		"DirectorySizeWarningPercent": cfg.DirectorySizeWarningPercent, "BackendFailureWarningPercent": cfg.BackendFailureWarningPercent,
	} {
		if percent < 0 || percent > 100 {
			errs = append(errs, fmt.Errorf("%w: %s must be between 0 and 100", ErrInvalidStatusConfig, name))
		}
	}
	slices.SortFunc(errs, func(a, b error) int { return strings.Compare(a.Error(), b.Error()) })
	return errors.Join(errs...)
}

// statusServiceConfigは、公開のStatusConfigを、statusのServiceの設定にします。0の項目は
// 既定値です。
func statusServiceConfig(cfg pkgadmin.StatusConfig) status.Config {
	defaults := pkgadmin.DefaultStatusConfig()
	pick := func(value, fallback float64) float64 {
		if value == 0 {
			return fallback
		}
		return value
	}
	result := status.Config{
		ScanInterval: cfg.DirectoryScanInterval,
		Thresholds: status.Thresholds{
			DiskFreeWarningRatio:      pick(cfg.DiskFreeWarningPercent, defaults.DiskFreeWarningPercent) / 100,
			DiskFreeCriticalRatio:     pick(cfg.DiskFreeCriticalPercent, defaults.DiskFreeCriticalPercent) / 100,
			DirectorySizeWarningRatio: pick(cfg.DirectorySizeWarningPercent, defaults.DirectorySizeWarningPercent) / 100,
			FailureRateWarningRatio:   pick(cfg.BackendFailureWarningPercent, defaults.BackendFailureWarningPercent) / 100,
			SlowRequest:               cfg.SlowRequest,
		},
	}
	if result.ScanInterval == 0 {
		result.ScanInterval = defaults.DirectoryScanInterval
	}
	if cfg.DisableDirectoryScan {
		result.ScanInterval = 0
	}
	if result.Thresholds.SlowRequest == 0 {
		result.Thresholds.SlowRequest = defaults.SlowRequest
	}
	return result
}

// NewHandlerは、管理用APIのhttp.Handlerを返します。
//
//	GET   /settings        全loggerの現在の設定を返します
//	PATCH /settings        JSONの本文で指定した項目を変更し、変更後の設定を返します
//	GET   /config          全loggerとtelemetryを作った設定を返します
//	GET   /config/<path>   設定の文書のpathの先の値だけを返します
//	GET   /config?path=<path>&path=<path>  複数のpathの先の値をまとめて返します
//	GET   /status          processと全memberの様子と、注意すべき状態を返します
//	GET   /status/<path>   statusの文書のpathの先の値だけを返します（?pathも同じ）
//	GET   /                ブラウザ画面（HTML）。/ui.cssと/ui.jsがその部品です
//
// ブラウザ画面のfile以外のすべての要求で「Authorization: Bearer <Token>」が必要です。PATCHは一つずつ順に処理し、
// 全loggerの全変更を検証してから反映して、一つでも不正なら何も反映しません。要求、変更を
// 始める時点の設定、変更後の設定はauditへ監査logとして記録し、拒否した要求も記録します。
// 記録しないのは、成功したGETだけです。変更は次のlogから反映し、メモリ上だけに保持します。
// loggersの出力先どうしが同じLiveSettingsを共有している場合は、outputsettingsの
// ErrSharedLiveSettingsを返します。
//
// /configの文書には、Membersのうちconfigview.Sourceを実装するもの（heterologが作った
// loggerとtelemetry）だけが入ります。秘密の値は伏せます。Membersの二つのmapに同じ名前が
// あれば、ErrDuplicateNameを返します。
//
// /statusの報告には、status.Memberを実装するmemberだけが入ります。probesは、CPU・memory・
// directoryのscan・diskの空きの読み取りで、nilの項目はその値を報告しません。cfg.Statusの
// 値が範囲外なら、ErrInvalidStatusConfigを返します。
func NewHandler(cfg pkgadmin.HandlerConfig, audit pkglogger.DeliveryLogger, probes status.Probes) (http.Handler, error) {
	if len(cfg.Token) < pkgadmin.MinTokenLength {
		return nil, ErrTokenTooShort
	}
	if err := ValidateStatusConfig(cfg.Status); err != nil {
		return nil, err
	}
	service, err := outputsettings.NewService(cfg.Members.Loggers, audit)
	if err != nil {
		return nil, err
	}
	sources, err := configSources(cfg.Members)
	if err != nil {
		return nil, err
	}
	config, err := configview.NewService(sources)
	if err != nil {
		return nil, err
	}
	return &handler{
		tokenDigest: sha256.Sum256([]byte(cfg.Token)),
		service:     service,
		config:      config,
		status:      status.NewService(statusMembers(cfg.Members), probes, statusServiceConfig(cfg.Status)),
	}, nil
}

// statusMembersは、Membersのうち様子を報告できるものを、名前ごとに集めます。
func statusMembers(members pkgadmin.Members) map[string]status.Member {
	result := map[string]status.Member{}
	add := func(name string, member any) {
		if m, ok := member.(status.Member); ok {
			result[name] = m
		}
	}
	for name, logger := range members.Loggers {
		add(name, logger)
	}
	for name, logger := range members.DeliveryLoggers {
		add(name, logger)
	}
	for name, telemetry := range members.Telemetries {
		add(name, telemetry)
	}
	return result
}

// configSourcesは、Membersのうち設定を示せるものを、名前ごとに集めます。
func configSources(members pkgadmin.Members) (map[string]configview.Source, error) {
	sources := map[string]configview.Source{}
	var errs []error
	add := func(name string, member any) {
		if _, taken := sources[name]; taken {
			errs = append(errs, fmt.Errorf("%w: %q", ErrDuplicateName, name))
			return
		}
		// 設定を示せないmember（SettingsControllerの独自の実装など）は、文書に入れません。
		// 名前の重複は、文書に入らないmemberでも見つけたいので、入れない場合も名前を取ります。
		source, _ := member.(configview.Source)
		sources[name] = source
	}
	for _, name := range slices.Sorted(maps.Keys(members.Loggers)) {
		add(name, members.Loggers[name])
	}
	for _, name := range slices.Sorted(maps.Keys(members.DeliveryLoggers)) {
		add(name, members.DeliveryLoggers[name])
	}
	for _, name := range slices.Sorted(maps.Keys(members.Telemetries)) {
		add(name, members.Telemetries[name])
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	maps.DeleteFunc(sources, func(_ string, source configview.Source) bool { return source == nil })
	return sources, nil
}

type handler struct {
	tokenDigest [sha256.Size]byte
	service     *outputsettings.Service
	config      *configview.Service
	status      *status.Service
}

// 管理用APIのJSONの文書です。
type (
	settingsDocument struct {
		Loggers map[string]loggerDocument `json:"loggers"`
		// Warningsは、変更は反映したが知らせることがある場合の説明です。
		Warnings []string `json:"warnings,omitempty"`
	}
	loggerDocument struct {
		Outputs map[pkglogger.LogOutputDestination]outputDocument `json:"outputs"`
	}
	outputDocument struct {
		Level    string           `json:"level"`
		Sampling samplingDocument `json:"sampling"`
	}
	samplingDocument struct {
		Enabled       bool `json:"enabled"`
		ByRequestID   bool `json:"by_request_id"`
		RequestIDRate int  `json:"request_id_rate"`
		DebugRate     int  `json:"debug_rate"`
		InfoRate      int  `json:"info_rate"`
		WarnRate      int  `json:"warn_rate"`
		ErrorRate     int  `json:"error_rate"`
	}
	patchDocument struct {
		Loggers map[string]loggerPatch `json:"loggers"`
	}
	loggerPatch struct {
		Outputs map[pkglogger.LogOutputDestination]outputPatch `json:"outputs"`
	}
	outputPatch struct {
		Level    *string        `json:"level"`
		Sampling *samplingPatch `json:"sampling"`
	}
	samplingPatch struct {
		Enabled       *bool `json:"enabled"`
		ByRequestID   *bool `json:"by_request_id"`
		RequestIDRate *int  `json:"request_id_rate"`
		DebugRate     *int  `json:"debug_rate"`
		InfoRate      *int  `json:"info_rate"`
		WarnRate      *int  `json:"warn_rate"`
		ErrorRate     *int  `json:"error_rate"`
	}
	errorDocument struct {
		Error string `json:"error"`
		// Loggersは、変更を反映した後の誤りで、今の設定を返す場合だけ含めます。
		Loggers  map[string]loggerDocument `json:"loggers,omitempty"`
		Warnings []string                  `json:"warnings,omitempty"`
	}
)

// warningAuditUndeliveredは、監査logの一部が記録先のすべてへ届かなかったときの説明です。
const warningAuditUndelivered = "some audit records of this request were not delivered to every audit output"

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// ブラウザ画面のfileは、tokenを持たないため認証の前に返します。画面はAPIを呼ぶだけで、
	// 設定もstatusも含みません。
	if h.serveUI(w, r) {
		return
	}
	// 要求IDは、応答のheaderと監査logの両方に含め、利用者が自分の要求の記録を探せる
	// ようにします。
	request := outputsettings.Request{ID: uuid.NewString(), RemoteAddr: r.RemoteAddr}
	w.Header().Set("X-Request-Id", request.ID)
	// 経路より先に認証し、tokenのない要求に経路の有無を知らせません。監査logが届いたか
	// どうかも、tokenのない要求には知らせません。
	if !h.authorized(r) {
		const reason = "missing or invalid bearer token"
		h.service.Reject(r.Context(), request, reason)
		w.Header().Set("WWW-Authenticate", `Bearer realm="heterolog"`)
		writeError(w, http.StatusUnauthorized, reason, false)
		return
	}
	if r.URL.Path == "/config" || strings.HasPrefix(r.URL.Path, "/config/") {
		h.serveDocument(w, r, request, "/config", h.config.Document)
		return
	}
	if r.URL.Path == "/status" || strings.HasPrefix(r.URL.Path, "/status/") {
		h.serveDocument(w, r, request, "/status", func() map[string]any {
			report := h.status.Report(r.Context())
			document, _ := configview.Render(report).(map[string]any)
			return document
		})
		return
	}
	if r.URL.Path != "/settings" {
		const reason = "not found"
		writeError(w, http.StatusNotFound, reason, h.service.Reject(r.Context(), request, reason))
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, newSettingsDocument(h.service.Settings()))
	case http.MethodPatch:
		h.patch(w, r, request)
	default:
		const reason = "method not allowed"
		w.Header().Set("Allow", "GET, PATCH")
		writeError(w, http.StatusMethodNotAllowed, reason, h.service.Reject(r.Context(), request, reason))
	}
}

// serveDocumentは、prefixの文書か、prefix/<path>の先の値か、prefix?path=...で指定した
// 複数のpathの先の値を返します。文書はdocumentで作ります。pathの各要素は文書のkeyか配列の
// 添字で、一つでも辿れなければ404にし、辿れなかったpathを理由に含めます。
func (h *handler) serveDocument(w http.ResponseWriter, r *http.Request, request outputsettings.Request, prefix string, document func() map[string]any) {
	if r.Method != http.MethodGet {
		const reason = "method not allowed"
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, reason, h.service.Reject(r.Context(), request, reason))
		return
	}
	if rest, ok := strings.CutPrefix(r.URL.Path, prefix+"/"); ok {
		value, found := configview.Lookup(document(), strings.Split(rest, "/"))
		if !found {
			const reason = "not found"
			writeError(w, http.StatusNotFound, reason, h.service.Reject(r.Context(), request, reason))
			return
		}
		writeJSON(w, http.StatusOK, value)
		return
	}
	paths := r.URL.Query()["path"]
	doc := document()
	if len(paths) == 0 {
		writeJSON(w, http.StatusOK, doc)
		return
	}
	values := make(map[string]any, len(paths))
	var missing []string
	for _, path := range paths {
		value, found := configview.Lookup(doc, strings.Split(strings.Trim(path, "/"), "/"))
		if !found {
			missing = append(missing, path)
			continue
		}
		values[path] = value
	}
	if len(missing) > 0 {
		reason := "not found: " + strings.Join(missing, ", ")
		writeError(w, http.StatusNotFound, reason, h.service.Reject(r.Context(), request, reason))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"values": values})
}

// authorizedは、tokenのdigestを一定時間で比較します。拒否までの時間から、tokenの
// 一致した長さもtokenの長さも分からないようにするためです。
func (h *handler) authorized(r *http.Request) bool {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return false
	}
	digest := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return subtle.ConstantTimeCompare(digest[:], h.tokenDigest[:]) == 1
}

func (h *handler) patch(w http.ResponseWriter, r *http.Request, request outputsettings.Request) {
	reject := func(status int, reason string) {
		writeError(w, status, reason, h.service.Reject(r.Context(), request, reason))
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	decoder.DisallowUnknownFields()
	var patch patchDocument
	if err := decoder.Decode(&patch); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			reject(http.StatusRequestEntityTooLarge, "request body is too large")
			return
		}
		reject(http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		reject(http.StatusBadRequest, "invalid JSON: the body must contain a single JSON object")
		return
	}
	changes, err := newChanges(patch)
	if err != nil {
		reject(http.StatusBadRequest, err.Error())
		return
	}
	result, err := h.service.Update(r.Context(), request, changes)
	if err != nil {
		writeError(w, statusOf(err), err.Error(), result.AuditUndelivered)
		return
	}
	var warnings []string
	if result.AuditUndelivered {
		warnings = append(warnings, warningAuditUndelivered)
	}
	settings := newSettingsDocument(h.service.Settings())
	if len(result.Mismatches) > 0 {
		// 変更は反映済みで、戻しません。今の設定を返し、要求どおりでないことを知らせます。
		writeJSON(w, http.StatusInternalServerError, errorDocument{
			Error:    "the settings were changed but do not match the request; they were not reverted",
			Loggers:  settings.Loggers,
			Warnings: warnings,
		})
		return
	}
	settings.Warnings = warnings
	writeJSON(w, http.StatusOK, settings)
}

// newChangesは、PATCHの文書を変更へ変換します。文書で省略したsamplingの項目は変更に
// 含めず、反映する時点の値のままにします。今の値をここで読んで補うと、同時のPATCHが
// 互いの変更を古い値で上書きするためです。loggerや出力先の有無は、Service.Updateが
// 確かめます。
func newChanges(
	patch patchDocument,
) (map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange, error) {
	changes := make(map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange, len(patch.Loggers))
	for _, name := range slices.Sorted(maps.Keys(patch.Loggers)) {
		outputs := map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{}
		for _, output := range slices.Sorted(maps.Keys(patch.Loggers[name].Outputs)) {
			p := patch.Loggers[name].Outputs[output]
			var change pkglogger.OutputSettingsChange
			if p.Level != nil {
				level, err := pkglogger.ParseLogLevel(*p.Level)
				if err != nil {
					return nil, fmt.Errorf("logger %q output %q: %w", name, output, err)
				}
				change.Level = &level
			}
			if s := p.Sampling; s != nil {
				change.Sampling = &pkglogger.SamplingChange{
					IsSamplingEnabled:        s.Enabled,
					EnableLoggingByRequestID: s.ByRequestID,
					LoggingByRequestIDRate:   s.RequestIDRate,
					SamplingDebugRate:        s.DebugRate,
					SamplingInfoRate:         s.InfoRate,
					SamplingWarnRate:         s.WarnRate,
					SamplingErrorRate:        s.ErrorRate,
				}
			}
			outputs[output] = change
		}
		changes[name] = outputs
	}
	return changes, nil
}

// statusOfは、Service.Updateのerrorに対応する状態codeを返します。
func statusOf(err error) int {
	switch {
	case errors.Is(err, outputsettings.ErrUnknownLogger), errors.Is(err, pkglogger.ErrUnknownOutput):
		return http.StatusNotFound
	case errors.Is(err, outputsettings.ErrApplyFailed):
		return http.StatusConflict
	default:
		return http.StatusBadRequest
	}
}

func newSettingsDocument(settings map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettings) settingsDocument {
	document := settingsDocument{Loggers: make(map[string]loggerDocument, len(settings))}
	for name, outputs := range settings {
		logger := loggerDocument{Outputs: make(map[pkglogger.LogOutputDestination]outputDocument, len(outputs))}
		for output, s := range outputs {
			logger.Outputs[output] = outputDocument{
				Level: s.Level.String(),
				Sampling: samplingDocument{
					Enabled:       s.Sampling.IsSamplingEnabled,
					ByRequestID:   s.Sampling.EnableLoggingByRequestID,
					RequestIDRate: s.Sampling.LoggingByRequestIDRate,
					DebugRate:     s.Sampling.SamplingDebugRate,
					InfoRate:      s.Sampling.SamplingInfoRate,
					WarnRate:      s.Sampling.SamplingWarnRate,
					ErrorRate:     s.Sampling.SamplingErrorRate,
				},
			}
		}
		document.Loggers[name] = logger
	}
	return document
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(body)
}

// writeErrorは、errorの文書を返します。auditUndeliveredは、この要求の監査logの一部が
// 記録先のすべてへ届かなかったことを表し、警告として含めます。
func writeError(w http.ResponseWriter, status int, message string, auditUndelivered bool) {
	document := errorDocument{Error: message}
	if auditUndelivered {
		document.Warnings = []string{warningAuditUndelivered}
	}
	writeJSON(w, status, document)
}
