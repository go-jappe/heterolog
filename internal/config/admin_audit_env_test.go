package config

import (
	"errors"
	"maps"
	"reflect"
	"strings"
	"testing"

	pkgenv "github.com/go-jappe/heterolog/pkg/env"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// 管理用serverを有効にしない場合は、監査用loggerの設定を読み込みません。
func TestAdminAuditConfigIsLoadedOnlyWithTheAdminServer(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{"HETEROLOG_ADMIN_AUDIT_LOG_TYPE=stdout"})
	if !reflect.DeepEqual(cfg.AdminAudit, AdminAuditConfig{}) {
		t.Errorf("AdminAudit = %+v without HETEROLOG_ADMIN_ADDR, want the zero value", cfg.AdminAudit)
	}
	if _, err := MapAdminAuditLoggerConfig(cfg); !errors.Is(err, ErrAdminAuditNotEnabled) {
		t.Errorf("MapAdminAuditLoggerConfig() error = %v, want ErrAdminAuditNotEnabled", err)
	}
}

// 監査用loggerだけを使う場合も、共通の一件のlogのbyte上限を読み込み、検証します。
func TestAdminAuditOnlyConfigLoadsAndValidatesMaxLogEntryBytes(t *testing.T) {
	for _, tt := range []struct {
		name    string
		value   string
		want    int
		wantErr error
	}{
		{name: "default", want: pkglogger.DefaultMaxLogEntryBytes},
		{name: "explicit", value: "1024", want: 1024},
		{name: "zero", value: "0", wantErr: pkglogger.ErrInvalidMaxLogEntryBytes},
		{name: "negative", value: "-1", wantErr: pkglogger.ErrInvalidMaxLogEntryBytes},
		{name: "not an integer", value: "invalid", wantErr: pkgenv.ErrNotInteger},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lines := []string{
				"HETEROLOG_ENABLE_LOGGING=false",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				"HETEROLOG_ADMIN_ADDR=127.0.0.1:9100",
				"HETEROLOG_ADMIN_AUDIT_LOG_TYPE=stdout",
			}
			if tt.value != "" {
				lines = append(lines, "HETEROLOG_MAX_LOG_ENTRY_BYTES="+tt.value)
			}
			cfg, err := loadConfigErrorFromTempEnv(t, lines)
			var mapped pkglogger.DeliveryLoggerConfig
			if err == nil {
				mapped, err = MapAdminAuditLoggerConfig(cfg)
			}
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("audit configuration error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("audit configuration error = %v", err)
			}
			if mapped.MaxLogEntryBytes != tt.want {
				t.Errorf("audit MaxLogEntryBytes = %d, want %d", mapped.MaxLogEntryBytes, tt.want)
			}
		})
	}
}

// 既定では、標準出力と、/audit_logsの一つのfileへ記録します。fileはlevelで分けず、保守で
// 消さず、圧縮もuploadもしません。Delivery loggerを無効にしても使え、Delivery loggerの
// file・internal logの保存先の設定は引き継ぎません。
func TestAdminAuditConfigDefaultsToStdoutAndOneAuditFile(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ADMIN_ADDR=127.0.0.1:9100",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_TARGET=audit-test",
		"HETEROLOG_SYSTEM_LOG_DIR=" + t.TempDir(),
		"HETEROLOG_DELIVERY_LOG_DIR=/delivery_logs",
		"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_SEPARATE_ERROR_LOGS=true",
		"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_SEPARATE_DEBUG_LOGS=true",
		"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_COMPRESS=true",
		"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_FILE_LOG_DIR=/delivery_internal_logs",
	})
	if !cfg.AdminAudit.Enabled || cfg.DeliveryLoggerEnabled {
		t.Fatalf("AdminAudit.Enabled = %v, DeliveryLoggerEnabled = %v; want the audit logger without the delivery logger",
			cfg.AdminAudit.Enabled, cfg.DeliveryLoggerEnabled)
	}
	mapped, err := MapAdminAuditLoggerConfig(cfg)
	if err != nil {
		t.Fatalf("MapAdminAuditLoggerConfig(): %v", err)
	}
	if got := mapped.LogOutputDestinations.String(); got != "stdout,file" {
		t.Errorf("outputs = %q, want stdout,file", got)
	}
	if mapped.Stdout == nil || mapped.File == nil {
		t.Fatalf("Stdout = %v, File = %v; want both outputs configured", mapped.Stdout, mapped.File)
	}
	layout := mapped.File.FsstoreConfig.Layout
	if layout.Target != "audit-test" || layout.LogDir != "/audit_logs" || layout.ErrorLogDir != "/audit_logs" ||
		layout.IsSeparateDebugLogDirEnabled || layout.IsSeparateErrorLogsEnabled || layout.IsSeparateDebugLogsEnabled {
		t.Errorf("file layout = %+v, want every level in one file of /audit_logs for the target", layout)
	}
	runner := mapped.File.LifecycleRunnerSettings
	if mapped.File.LifecycleMode != pkgfsstore.FileLifecycleModeDisable || runner.EnableFileCompress || runner.ObjectStorageUploadConfig.Enabled {
		t.Errorf("file lifecycle = %q, compress = %v, upload = %v; want no maintenance, compression or upload",
			mapped.File.LifecycleMode, runner.EnableFileCompress, runner.ObjectStorageUploadConfig.Enabled)
	}
	if mapped.InternalLogDestinations != nil {
		t.Errorf("InternalLogDestinations = %+v, want none inherited from the delivery logger", mapped.InternalLogDestinations)
	}
}

// 出力先ごとの記録の場所は監査用の値に変わり、failure archiveは監査用のdirectoryを使って
// uploadしません。Delivery loggerの設定は変わりません。
func TestAdminAuditConfigUsesItsOwnIdentityAndFailureArchives(t *testing.T) {
	lines := []string{
		"HETEROLOG_ADMIN_ADDR=127.0.0.1:9100",
		"HETEROLOG_SYSTEM_LOG_DIR=" + t.TempDir(),
		"HETEROLOG_DELIVERY_LOG_TYPE=stdout,slack,opensearch,loki,cloudwatchlogs,clickhouse",
		"HETEROLOG_ADMIN_AUDIT_LOG_TYPE=stdout,slack,opensearch,loki,cloudwatchlogs,clickhouse",
		"HETEROLOG_DELIVERY_SLACK_TOKEN=xoxb-delivery",
		"HETEROLOG_DELIVERY_SLACK_NORMAL_CHANNEL=C0DELIVERY",
		"HETEROLOG_DELIVERY_SLACK_ERROR_CHANNEL=C0DELIVERYERR",
		"HETEROLOG_DELIVERY_OPENSEARCH_ENDPOINT=http://127.0.0.1:9200",
		"HETEROLOG_DELIVERY_OPENSEARCH_INDEX=delivery-index",
		"HETEROLOG_DELIVERY_LOKI_ENDPOINT=http://127.0.0.1:3100",
		"HETEROLOG_DELIVERY_CLOUDWATCH_REGION=ap-northeast-1",
		"HETEROLOG_DELIVERY_CLOUDWATCH_LOG_GROUP_NAME=/delivery/group",
		"HETEROLOG_DELIVERY_CLOUDWATCH_LOG_STREAM_NAME=delivery-stream",
		"HETEROLOG_DELIVERY_CLICKHOUSE_ENDPOINT=http://127.0.0.1:8123",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DATABASE=delivery_db",
		"HETEROLOG_DELIVERY_CLICKHOUSE_TABLE=delivery_table",
		"HETEROLOG_ADMIN_AUDIT_SLACK_CHANNEL=C0AUDIT",
		"HETEROLOG_ADMIN_AUDIT_OPENSEARCH_INDEX=audit-index",
		`HETEROLOG_ADMIN_AUDIT_LOKI_LABELS={"stream":"admin_audit"}`,
		"HETEROLOG_ADMIN_AUDIT_CLOUDWATCH_LOG_GROUP_NAME=/audit/group",
		"HETEROLOG_ADMIN_AUDIT_CLOUDWATCH_LOG_STREAM_NAME=audit-stream",
		"HETEROLOG_ADMIN_AUDIT_CLICKHOUSE_DATABASE=audit_db",
		"HETEROLOG_ADMIN_AUDIT_CLICKHOUSE_TABLE=audit_table",
	}
	cfg := loadConfigFromTempEnv(t, lines)
	audit := ToAdminAuditLoggerConfig(cfg)
	if audit.Slack == nil || audit.OpenSearch == nil || audit.Loki == nil || audit.CloudWatchLogs == nil ||
		audit.CloudWatchLogsClient == nil || audit.ClickHouseClient == nil {
		t.Fatalf("audit config = %+v, want every remote output configured", audit)
	}
	for _, channel := range []string{audit.Slack.Channel, audit.Slack.ErrorChannel, audit.Slack.WarnChannel, audit.Slack.InfoChannel, audit.Slack.DebugChannel} {
		if channel != "C0AUDIT" {
			t.Errorf("Slack channels = %+v, want C0AUDIT for every level", audit.Slack)
			break
		}
	}
	if got := audit.OpenSearch.Index; got != "audit-index" {
		t.Errorf("OpenSearch index = %q, want audit-index", got)
	}
	if got := audit.Loki.StaticLabels; !maps.Equal(got, map[string]string{"stream": "admin_audit"}) {
		t.Errorf("Loki labels = %v, want stream=admin_audit", got)
	}
	if audit.CloudWatchLogs.LogGroupName != "/audit/group" || audit.CloudWatchLogs.LogStreamName != "audit-stream" ||
		audit.CloudWatchLogsClient.LogGroupName != "/audit/group" {
		t.Errorf("CloudWatch Logs = %q/%q, client group %q; want /audit/group/audit-stream",
			audit.CloudWatchLogs.LogGroupName, audit.CloudWatchLogs.LogStreamName, audit.CloudWatchLogsClient.LogGroupName)
	}
	if audit.ClickHouseClient.Database != "audit_db" || audit.ClickHouseClient.Table != "audit_table" {
		t.Errorf("ClickHouse = %q.%q, want audit_db.audit_table", audit.ClickHouseClient.Database, audit.ClickHouseClient.Table)
	}
	for destination, archive := range map[pkglogger.LogOutputDestination]string{
		pkglogger.LogOutputOpenSearch:     audit.OpenSearch.FailureStoreConfig.FileArchiveConfig.Dir,
		pkglogger.LogOutputLoki:           audit.Loki.FailureStoreConfig.FileArchiveConfig.Dir,
		pkglogger.LogOutputCloudWatchLogs: audit.CloudWatchLogs.FailureStoreConfig.FileArchiveConfig.Dir,
		pkglogger.LogOutputClickHouse:     audit.ClickHouse.FailureStoreConfig.FileArchiveConfig.Dir,
	} {
		if want := "admin_audit_logger_" + destination.String() + "_failure_archives"; !strings.HasSuffix(archive, want) {
			t.Errorf("%s failure archive = %q, want the audit directory %s", destination, archive, want)
		}
	}
	for destination, upload := range map[pkglogger.LogOutputDestination]bool{
		pkglogger.LogOutputOpenSearch:     audit.OpenSearch.FailureStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig.Enabled,
		pkglogger.LogOutputLoki:           audit.Loki.FailureStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig.Enabled,
		pkglogger.LogOutputCloudWatchLogs: audit.CloudWatchLogs.FailureStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig.Enabled,
		pkglogger.LogOutputClickHouse:     audit.ClickHouse.FailureStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig.Enabled,
	} {
		if upload {
			t.Errorf("%s failure archive uploads, want no upload for the audit logger", destination)
		}
	}

	delivery := ToDeliveryLoggerConfig(cfg)
	for name, got := range map[string][2]any{
		"Slack channel":                {delivery.Slack.Channel, "C0DELIVERY"},
		"Slack error channel":          {delivery.Slack.ErrorChannel, "C0DELIVERYERR"},
		"OpenSearch index":             {delivery.OpenSearch.Index, "delivery-index"},
		"Loki labels":                  {len(delivery.Loki.StaticLabels), 0},
		"CloudWatch Logs group":        {delivery.CloudWatchLogs.LogGroupName, "/delivery/group"},
		"CloudWatch Logs client group": {delivery.CloudWatchLogsClient.LogGroupName, "/delivery/group"},
		"ClickHouse table":             {delivery.ClickHouseClient.Table, "delivery_table"},
		"Loki failure archive":         {strings.Contains(delivery.Loki.FailureStoreConfig.FileArchiveConfig.Dir, adminAuditLoggerLabel), false},
	} {
		if got[0] != got[1] {
			t.Errorf("delivery %s = %v, want %v unchanged by the audit identity", name, got[0], got[1])
		}
	}
}

// 監査logのdirectoryが、ほかの保存先のdirectoryと重なる設定は拒否します。
func TestAdminAuditFileDirectoryMustNotOverlapAnotherStore(t *testing.T) {
	_, err := loadConfigErrorFromTempEnv(t, []string{
		"HETEROLOG_ADMIN_ADDR=127.0.0.1:9100",
		"HETEROLOG_SYSTEM_LOG_DIR=" + t.TempDir(),
		"HETEROLOG_DELIVERY_LOG_TYPE=stdout,file",
		"HETEROLOG_ADMIN_AUDIT_FILE_LOG_DIR=/delivery_logs",
	})
	var collision *StorageDirectoryCollisionError
	if !errors.As(err, &collision) || !strings.Contains(err.Error(), adminAuditLoggerLabel+".file") {
		t.Fatalf("validation error = %v, want a storage directory collision naming %s.file", err, adminAuditLoggerLabel)
	}
}

// Delivery loggerを無効にしても、監査logの出力先に使うbackendのclient設定は、
// HETEROLOG_DELIVERY_*から読み込みます。
func TestAdminAuditLoadsDeliveryClientsForItsOutputs(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ADMIN_ADDR=127.0.0.1:9100",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_SYSTEM_LOG_DIR=" + t.TempDir(),
		"HETEROLOG_ADMIN_AUDIT_LOG_TYPE=loki",
		"HETEROLOG_DELIVERY_LOKI_ENDPOINT=http://127.0.0.1:3100",
	})
	mapped, err := MapAdminAuditLoggerConfig(cfg)
	if err != nil {
		t.Fatalf("MapAdminAuditLoggerConfig(): %v", err)
	}
	if mapped.LokiClient == nil || mapped.LokiClient.Endpoint != "http://127.0.0.1:3100" {
		t.Errorf("Loki client = %+v, want the delivery Loki endpoint", mapped.LokiClient)
	}
}

// 監査用loggerも、Delivery loggerと同じく、親timeoutとclientのtimeoutの階層を検証します。
func TestAdminAuditTimeoutHierarchyIsValidated(t *testing.T) {
	_, err := loadConfigErrorFromTempEnv(t, []string{
		"HETEROLOG_ADMIN_ADDR=127.0.0.1:9100",
		"HETEROLOG_SYSTEM_LOG_DIR=" + t.TempDir(),
		"HETEROLOG_DELIVERY_LOG_TYPE=stdout",
		"HETEROLOG_ADMIN_AUDIT_LOG_TYPE=loki",
		"HETEROLOG_DELIVERY_LOKI_ENDPOINT=http://127.0.0.1:3100",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_SECOND=1",
		"HETEROLOG_DELIVERY_LOKI_TIMEOUT_SECOND=30",
	})
	if err == nil || !strings.Contains(err.Error(), adminAuditLoggerLabel+":") {
		t.Fatalf("validation error = %v, want a timeout hierarchy error of the audit logger", err)
	}
}
