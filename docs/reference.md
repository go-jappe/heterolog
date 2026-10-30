# heterolog reference

[日本語版](reference.ja.md)

This document describes the behavior of heterolog in detail: what each API returns, how settings are checked, how log files and failed logs are handled, and what each destination does. For an overview, installation, and examples, see the [README](../README.md). The API itself is documented on [pkg.go.dev](https://pkg.go.dev/github.com/go-jappe/heterolog).

## Contents

- [Writing logs](#writing-logs)
- [Loggers and shutdown](#loggers-and-shutdown)
- [Configuration](#configuration)
- [Log files](#log-files)
- [Failure handling and replay](#failure-handling-and-replay)
- [Sending traces](#sending-traces)
- [Metrics and traces](#metrics-and-traces)
- [Destination notes](#destination-notes)
- [Log recovery CLI](#log-recovery-cli)
- [Testing](#testing)

## Writing logs

### Errors from logging calls

The regular API returns errors from log processing.

```go
jobID := heterolog.MustFieldKey("job_id")
err := runtime.Info(ctx, "job completed", jobID.Int(42))
```

### Field values

- `Any(key, value)` keeps the value as JSON when the field is created.
- A value that cannot be converted to JSON, or whose arrays and objects are nested deeper than `MaxFieldNestingDepth` (9000 levels), makes field creation return `ErrFieldValueNotJSON`. This limit is the depth that can still be stored and replayed after the spool and each destination's format add their own outer levels.
- The same limit applies to slog attributes. A record that exceeds it is not written, and `Handle` returns an error.
- An `ErrFieldValueNotJSON` error includes the reason the value could not be converted (such as the error returned by `MarshalJSON`) in its message only. Because that reason can contain the value being logged, it cannot be retrieved with `errors.Is` or `errors.As`.

### Metadata keys

- `request_id`, `trace_id`, and `span_id` are treated as log metadata when their values are strings.
- Numbers, booleans, objects, and other values keep their type and are stored as regular structured fields.
- When the same key is given more than once, only the last value is used, and it is classified by its type.
- These rules also apply to top-level `slog` attributes.

### Stack traces

- `ErrorWithStack(ctx, message, err, fields...)` records the application's stack at the call.
- The caller is kept even when the log is processed asynchronously, and the stack is shown according to each destination's `ErrorTraceDepth`. Depth 0 shows only the call site; depth N shows N parent frames above it. Debug mode uses `ErrorTraceDepthDebugMode`.
- A `nil` `err` is not rejected; the log is recorded as an ERROR log without error details, together with the stack.

### Using slog

- `slog.Logger` does not return the result of `Handler.Handle` to the caller. Use the regular API to check errors from a call, and `NewDelivery` to check delivery for each destination.
- A successful call of the regular API can mean that the log was accepted into a buffer; it does not guarantee final delivery.
- `slog` and each backend have their own level filters. To write DEBUG logs, set both:

```go
cfg := heterolog.DefaultConfig()
cfg.Stdout.RuntimeConfig.LogLevel = heterolog.LevelDebug
runtime, err := heterolog.New(ctx, cfg)
// Check err before using runtime.
logger := slog.New(heterolog.NewSlogHandler(runtime, &slog.HandlerOptions{
    Level: slog.LevelDebug,
    AddSource: true,
}))
```

- `WithAttrs`, `WithGroup`, `LogValuer`, `LevelVar`, and `ReplaceAttr` are supported.
- slog attributes are normalized to JSON-equivalent values and stored as structured fields. When a key appears more than once, the later value is used.
- Replacing or removing the message with `ReplaceAttr` also changes the message sent to backends. A removed message becomes an empty string, and a message replaced by a non-string value becomes its JSON representation. The level and time of the original record are kept.

## Loggers and shutdown

### APIs

| API | Purpose |
| --- | --- |
| `DefaultConfig` / `New` | Synchronous API. Returns processing results as errors (a drop caused by overload is `ErrLogDropped`) |
| `DefaultAsyncConfig` / `NewAsync` | Asynchronous best-effort logging |
| `DefaultDeliveryConfig` / `NewDelivery` | Returns the delivery result for each destination (ClickHouse reports acceptance into its queue as `DeliveryEnqueued`) |
| `NewSlogHandler` | Uses a synchronous logger as a `slog.Handler` |
| `DefaultSpoolConfig` / `WithSpool` | Settings for the workers that replay persisted failed logs |
| `Close` / `Shutdown(ctx)` | Finishes accepted work, flushes buffers, and releases resources |
| `ForceClose` | Stops accepting logs, releases waiting `Close` and `Shutdown` calls with `ErrForcedClose`, and requests background and recovery work to stop without waiting |

### Delivery reports

Each destination in a report has one of these statuses:

| Status | Meaning |
| --- | --- |
| `DeliverySucceeded` | The destination confirmed success |
| `DeliveryFailed` | The destination explicitly rejected the log |
| `DeliveryRejected` | The log was rejected before sending, for example by input validation |
| `DeliveryUnconfirmed` | The result cannot be confirmed, for example because the response was missing or could not be parsed |
| `DeliveryEnqueued` | Only acceptance into the send queue is reported |

- ClickHouse is designed for batch inserts, so even the delivery API returns `DeliveryEnqueued` as soon as the log is accepted into the queue, without waiting for the send. The send happens when the count, size, or `FlushInterval` threshold is reached, or at `Close`, and its result is not reflected in the returned report.
- `HasTargetFailure()` counts only confirmed failures and rejections before sending. To stop on unconfirmed results as well, use `HasUnregisteredTarget()`. `DeliveryEnqueued` is treated as unconfirmed, so `HasUnregisteredTarget()` is `true` for a report that includes ClickHouse.
- Saving a log to fallback storage is independent of success at the destination.
- For a report without any destination result, `HasTargetFailure()` and `HasAnyFailure()` are `true`, just as `Error()` returns `ErrNoDeliveryTargets`. A configuration without outputs is rejected at construction, so such a report can only come from an internal inconsistency.
- The `Backend` of a destination result is the name of a `pkg/logger.LogOutputDestination` (`stdout`, `file`, `slack`, `opensearch`, `loki`, `cloudwatchlogs`, `clickhouse`), whether the destination returned the result or the logger created it on its behalf (a handler panic or deadline, or a rejection before sending because `Close` had started or a single entry exceeded the limit).
- When a panic in a delivery handler is recovered, destinations that did not return a result are `DeliveryUnconfirmed`. Confirmed results of other destinations are kept.
- CloudWatch Logs send errors keep the original cause in the returned error and in the destination's `Err`, and can be identified with `errors.Is` and `errors.As`.

### Errors and dropped logs

- With the regular API of OpenSearch, Loki, ClickHouse, and CloudWatch Logs, when a log that failed to send could not be persisted (including when no fallback is configured), the returned error contains both the persistence error and the destination's original cause, which can be identified with `errors.Is` and `errors.As`. For ClickHouse, the error is returned by the logging call that ran the flush, or by `Close`.
- When a log that failed to send is discarded because its storage (spool or failure archive) is overloaded and the overload policy is `drop` (the default), the regular API returns an error that wraps `heterolog.ErrLogDropped`; check it with `errors.Is(err, heterolog.ErrLogDropped)`. The same error also contains the destination's original cause.
- When a buffered log is discarded by a later flush, the logging call that ran the flush, or `Close`, returns the error.
- When the log is saved by `degrade`, no error is returned.
- Logging calls of the asynchronous API do not return errors. If logs are discarded in the final flush of `Close`, `Close` returns the error.
- With the delivery API, check `DeliveryReport.HasDrop()`.
- The number of discarded logs can also be observed with the `heterolog_failure_records_total` metric with `action="dropped"`.

### Shutdown

- Always call `Close` or `Shutdown` when the application exits.
- When the context of `Shutdown` ends, only that call stops waiting; the cleanup continues. Call `Close` or `Shutdown` again to wait for it to finish.
- Unsent data in the spool may remain for the next replay. There is no separate `Flush` API.
- While compression or upload of log files keeps failing, `Close` does not return until that work completes. If you need an upper bound on shutdown time, stop waiting with the deadline of `Shutdown(ctx)`, and stop the cleanup with `ForceClose` if needed.
- `Close` and `Shutdown` wait for accepted sends and for the internal log notifications they cause (writes to the targets selected with `OnErrorConfig` and the like), including flushes that started inside logging calls because a count or size threshold was reached.
- When a panic is recovered while closing a destination, the other destinations are still closed, and incomplete closing is retried. A destination that is already closed is not closed again.

## Configuration

### Configuration types

- `Config`, `AsyncConfig`, and `DeliveryConfig` are the public configuration types of `pkg/logger`. The destinations are stdout, file, Slack, OpenSearch, Loki, CloudWatch Logs, and ClickHouse. See `go doc github.com/go-jappe/heterolog/pkg/logger` for the details of each setting.
- For synchronous and asynchronous configurations that use a spool, also pass `WithSpool`.
- A configuration without any regular output is rejected by `New`, `NewAsync`, and `NewDelivery` before construction, with an error that contains `ErrNoActiveLogOutput` of `pkg/logger` (check it with `errors.Is`).
- When `LogOutputDestinations` is empty, every non-nil backend configuration becomes a regular output. Storage or client settings used only for internal logs do not make a destination an output.

### Construction errors

Configuration errors at construction can be identified with `errors.Is` and `errors.As`:

- A nil context: `ErrNilContext`.
- A `Timeout` of 0 or less, and a `MaxParallel` of 0 or less for `NewAsync`: `pkg/logger.ConfigFieldError` with `Field` set to `heterolog.ConfigFieldTimeout` or `heterolog.ConfigFieldMaxParallel` and the kind `ErrNonPositiveSetting`.
- `WithSpool` passed to `NewDelivery`: `ErrDeliverySpoolUnsupported`.
- An unsupported value of `WithCloudWatchLogsStreamRouting`: `ErrInvalidStreamRouting` (`*InvalidStreamRoutingError`).
- A context that has already ended: its error, such as `context.Canceled`.
- A destination selected as an internal log notification target (`OnErrorConfig` and the like) whose settings cannot create a notification writer (an invalid `TimeZone`, an empty `LogLineDateFormat`, and so on): `ErrInvalidNotificationTarget` (`*NotificationTargetError`, where `Source` is the kind of logger, `Backend` is the target, and `Err` is the cause).
- Invalid presentation settings or backend values (buffers, send limits, retry settings, and so on): `pkg/logger.ConfigFieldError` with `Source`, `Backend`, and `Field` (and `Value` and `Limit` for checks against a value). The kind is a sentinel such as `ErrNonPositiveSetting` that can be checked with `errors.Is`.
- Invalid client settings: `Field` is `ConfigFieldClient` and the kind is `ErrInvalidClientConfig`. The reason is a sentinel of each backend package (for example, `pkg/loki.ErrEndpointRequired`) that can be checked with `errors.Is`.
- An endpoint that cannot be parsed as a URL: the error message and the `*url.Error` in the error tree contain only the URL with its user info (user name and password) replaced by `xxxxx`.
- A handler selector that cannot be resolved: `pkg/logger.HandlerSelectorError`.
- An invalid element of Slack's `Intervals`: its position is in `Index` of `pkg/logger.SlackIntervalError`, which can be retrieved with `errors.As`.
- The policy for running out of disk space while writing a file: a policy other than `drop` or `degrade` is `pkg/fsstore.ErrUnsupportedOverloadPolicy`, and a `degrade` destination other than `stdout` is `pkg/fsstore.ErrUnsupportedSpillDestination` (check them with `errors.Is`).

### Side effects and contexts

- Importing the package or calling `New` does not load `.env` files, install signal handlers, start servers, or change the global logger or OpenTelemetry providers. Configuration is passed explicitly in code (to build loggers from environment variables and an env file, call `LoadEnvConfig` explicitly, as described below).
- Do not change the configuration a logger uses after creating the logger.
- The cancellation and deadline of the context passed to `New`, `NewAsync`, and `NewDelivery` apply only to construction. Periodic sending of buffers continues after the context ends, until `Close` or `Shutdown`.

### Configuring from environment variables

`LoadEnvConfig` reads settings from environment variables that start with `HETEROLOG_` and from the env file given as its argument. Except for the AWS standard variables used as defaults (such as `AWS_ACCESS_KEY_ID`), every key starts with `HETEROLOG_`, so the settings can share the environment variables and file with the application's own settings. Every key is listed in [.env.example](../.env.example). Its values are a sample configuration and differ from the defaults in places; the default levels and sampling of each output are in [Default levels and sampling](../README.md#default-levels-and-sampling) of the README. With no settings, all three loggers write to stdout only. The recovery CLI reads the same keys.

An example is in [Sending logs to several destinations](../README.md#sending-logs-to-several-destinations) of the README.

- The process environment is not modified. When a key is in both the environment and the file, the environment value is used.
- No other file is read (neither `HETEROLOG_ENV_PATH` nor `.env` in the current directory).
- The settings of the enabled loggers and their consistency with each other (such as shared storage directories) are checked when loading, and all problems found are returned together. Use `errors.As` with `EnvFileError`, `InvalidEnvironmentValueError`, `RenamedEnvironmentKeyError`, or `VariableError` of `pkg/env` to identify the key.
- To adjust the settings before constructing a logger, get them with `cfg, opts, err := env.Config()` (or `AsyncConfig` and `DeliveryConfig`) and pass them to `heterolog.New(ctx, cfg, opts...)`. `opts` carries the spool replay settings and CloudWatch Logs stream routing read from the environment.
- A logger disabled with `HETEROLOG_ENABLE_LOGGING=false` or the like cannot be constructed, and `ErrEnvLoggerDisabled` is returned.
- Loggers constructed from the environment do not trace by themselves. Construct a `Telemetry` with `env.NewTelemetry(ctx)`, which reads `HETEROLOG_ENABLE_TRACES` and `HETEROLOG_TRACES_*` (see [Sending traces](#sending-traces)), and pass its `Tracer` as an additional option, as in `env.New(ctx, heterolog.WithObservers(tel.Tracer(), nil))`. A spool enabled with `HETEROLOG_USE_MEMORY_PRESSURE_*` judges memory pressure with `HETEROLOG_MEMORY_CRITICAL_RATIO`, `HETEROLOG_MIN_AVAILABLE_MEMORY_BYTES_MB` and `HETEROLOG_MEMORY_CRITICAL_CONSECUTIVE_HITS` unless `WithMemoryPressureMonitor` is passed.

### Changing settings while running

The level and sampling of each output of a synchronous or asynchronous logger can change while the application runs, in code or through an HTTP API.

- In code, `runtime.OutputSettings()` returns the current settings, and `runtime.UpdateOutputSettings(changes)` changes them and returns the settings of each changed output right before and after the change (`AppliedOutputSettings`). A change sets only the level and the sampling fields it gives (`OutputSettingsChange` with a `SamplingChange`); the others keep the values in effect when the change is applied, so changes made at the same time, in code or through the HTTP API, do not undo each other. All changes are checked first, and none is applied if any is invalid: an output the logger does not use is `ErrUnknownOutput`, and an invalid value is a `ConfigFieldError` of `pkg/logger`.
- Changes take effect from the next entry. An asynchronous logger uses the settings in effect when it processes an entry.
- Changes are kept in memory only, so a restart returns to the configured settings.
- Delivery loggers have no such settings: they do not sample, and only their Slack output filters by level, at its configured level. Outputs used only for internal log notifications keep their configured settings.

The HTTP API (admin API) can be called with tools such as curl:

- `NewAdminHandler` returns an `http.Handler` to mount on your own server. `StartAdminServer` starts a dedicated server instead. Both take the delivery logger that records the [audit log](#audit-log-of-the-admin-api).
- Each output of the loggers given to the admin API must have its own settings. If two outputs share one `LiveSettings` of `pkg/logger` (set through `RuntimeConfig.Live`), in one logger or in different loggers, or one logger is given under two names, `NewAdminHandler` and `StartAdminServer` return `ErrAdminSharedLiveSettings`: a change of one output would also change the other, which the audit log could neither record nor verify. Loggers created by heterolog are checked; other implementations of `SettingsController` are not. Loggers built from the environment configuration never share settings.
- `GET /settings` returns the current settings of every logger, and `PATCH /settings` changes the fields given in the JSON body. Levels are `DEBUG`, `INFO`, `WARN`, and `ERROR` (case-insensitive), and sampling fields omitted in a `PATCH` keep their values, also when another `PATCH` changes other fields at the same time.
- Every request must send `Authorization: Bearer <token>`. The token must be at least 32 bytes (`MinAdminTokenLength`); generate it randomly, for example with `openssl rand -hex 32`. It is compared in constant time, and errors never contain it.
- `PATCH` requests are processed one at a time; a request that arrives during another waits for it. A `PATCH` checks every change of every logger before applying any of them. The body is limited to 64 KiB, and unknown fields are rejected.
- Every response has the header `X-Request-Id`, the ID of the request in the audit log.
- The dedicated server listens on 127.0.0.1 when the host of its address is empty (for example, `:9100`). To listen on another address, give the host explicitly and set a TLS certificate and key, so the token is not sent over a network in clear text.
- From the environment configuration, `HETEROLOG_ADMIN_ADDR` enables the server, together with `HETEROLOG_ADMIN_TOKEN`, `HETEROLOG_ADMIN_TLS_CERT_FILE`, and `HETEROLOG_ADMIN_TLS_KEY_FILE`. `LoadEnvConfig` checks them, and `env.AdminServerConfig()` returns them.

Examples with curl are in [Changing the level and sampling while running](../README.md#changing-the-level-and-sampling-while-running) of the README.

### Audit log of the admin API

The admin API records every change request in an audit log, written by a delivery logger of its own, so that the changes can be checked later and are not hidden by the level and sampling that the API itself changes.

- Each `PATCH` is recorded in these steps, linked by the request ID:

  | Step | Level | Recorded when |
  | --- | --- | --- |
  | `requested` | INFO | the request arrives, with the requested fields (`changes`) |
  | `started` | INFO | the change starts, with the settings of the outputs to change (`settings`) |
  | `applied` | INFO | the change is applied, with the settings after it (`settings`) and whether they match the request (`verified`) |
  | `mismatch` | ERROR | for each output whose settings do not match the settings at the start with the request applied (`expected`, `actual`) |
  | `rejected` | WARN | a request is rejected, with the reason (`reason`); this includes requests with a missing or wrong token, and requests to other paths and methods |
  | `failed` | ERROR | a change cannot be applied after it was checked |
  | `undelivered` | ERROR | some records of the request did not reach every output of the audit logger (`undelivered_phases`) |

- Every record has the fields `log_kind=audit`, `audit_phase`, and `remote_addr`, and the request ID as the request ID of the log entry. Each output writes them in its own format, for example one text line in a file or a JSON document in OpenSearch. Only a successful `GET` is not recorded.
- A mismatch is not reverted: the response is `500` with the current settings. Undelivered records do not stop the change either: the response is still the result of the change and carries a warning, and the delivery logger keeps the records in its failure archive as usual.
- From the environment configuration, `env.NewAdminAudit(ctx)` builds the audit logger when `HETEROLOG_ADMIN_ADDR` is set; `env.AdminAuditConfig()` returns its settings. In code, pass a delivery logger created by heterolog (`*DeliveryRuntime`) to `NewAdminHandler` or `StartAdminServer`, and close it after the admin API stops.

The audit logger built from the environment uses the settings of the delivery logger (`HETEROLOG_DELIVERY_*`) for endpoints, credentials, and timeouts, even when `HETEROLOG_ENABLE_DELIVERY_LOGGING` is false, and changes only these:

- Outputs: `HETEROLOG_ADMIN_AUDIT_LOG_TYPE`. Unlike the other loggers, it defaults to both `stdout` and `file`, because the audit log must not be lost. Adding `slack` notifies the ERROR records, since the Slack output of a delivery logger records only ERROR by default.
- File: `HETEROLOG_ADMIN_AUDIT_FILE_LOG_DIR`, by default `/audit_logs`, resolved against `HETEROLOG_SYSTEM_LOG_DIR` and `HETEROLOG_TARGET` like the other log directories. All levels are written to the same file, so the records of a request are not split, and the files are not compressed, uploaded, or removed by maintenance. The directory must not overlap the directory of another store; `LoadEnvConfig` reports a `StorageDirectoryCollisionError`.
- Where each output records: `HETEROLOG_ADMIN_AUDIT_SLACK_CHANNEL` (every level), `HETEROLOG_ADMIN_AUDIT_OPENSEARCH_INDEX`, `HETEROLOG_ADMIN_AUDIT_LOKI_LABELS` (a JSON object of labels), `HETEROLOG_ADMIN_AUDIT_CLOUDWATCH_LOG_GROUP_NAME`, `HETEROLOG_ADMIN_AUDIT_CLOUDWATCH_LOG_STREAM_NAME`, `HETEROLOG_ADMIN_AUDIT_CLICKHOUSE_DATABASE`, and `HETEROLOG_ADMIN_AUDIT_CLICKHOUSE_TABLE`. Empty values keep those of the delivery logger; set them to keep the audit log apart from other logs.
- Failure archives use their own directories, `admin_audit_logger_<backend>_failure_archives`, and are not uploaded. The dedicated internal-log destinations of the delivery logger (`HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_*`) are not used.

### Memory pressure monitor

- The caller starts and stops the monitor passed to `WithMemoryPressureMonitor`.
- Without `WithMemoryPressureMonitor`, each spool that enables `UseMemoryPressureForOverload` starts its own monitor. It reads the memory of the operating system every second, judges each reading with the spool's `MemoryCriticalRatio`, `MinAvailableMemoryBytes` and `MemoryCriticalConsecutiveHits` (`HETEROLOG_MEMORY_CRITICAL_RATIO`, `HETEROLOG_MIN_AVAILABLE_MEMORY_BYTES_MB` and `HETEROLOG_MEMORY_CRITICAL_CONSECUTIVE_HITS` in the environment), and stops when the logger closes. The logger fails to start when they enable no threshold or the memory cannot be read.
- `NewMemoryPressureMonitor(provider, policy, pollInterval)` creates a monitor from a `Provider` and a `CriticalPolicy` of `pkg/memorypressure`.
- `Start(ctx)` reads once before returning, then reads every `pollInterval`, and stops when `ctx` ends.
- The evaluation is the same as `memorypressure.CriticalState`: the state is critical when reliable observations reach `CriticalRatio` or `MinAvailableBytes` `ConsecutiveCriticalHits` times in a row (0 and 1 mean the first time).
- A nil provider, a poll interval of 0 or less, a policy that enables neither threshold, a `CriticalRatio` above 1, or a negative count is an error at creation. Input errors at creation can be identified with `errors.Is(err, heterolog.ErrInvalidMemoryPressureMonitor)`.
- The monitor, passed or started by the spool, is used for the overload decision of destinations whose spool enables `UseMemoryPressureForOverload`. When it reports overload, the overload policy applies; with `drop`, the log is discarded without being saved, and the regular API returns `ErrLogDropped`.
- The Linux provider of `pkg/memorypressure` checks the limits of its cgroup v2 and of the parent levels it can read. It keeps the level with the highest usage as the main observation and the level with the least remaining memory as `CgroupLimit`, and checks both. When only a finite limit can be read, the usage is kept as unknown. Limits of ancestors it cannot read, such as those outside its namespace, cannot be obtained.

### Internal log notifications

- Notification targets (`OnErrorConfig`, `OnWarnConfig`, `OnInfoConfig`, and `OnDebugConfig` of `HandlerConfig`) use write paths dedicated to notifications.
  - `file` writes through a path that does not notify the file logger's internal logs.
  - `stdout`, `slack`, `opensearch`, `loki`, `cloudwatchlogs`, and `clickhouse` use notification loggers created separately from the regular outputs.
  - Failures of these writes are not notified; each failure is written as one line to the standard logger.
- The settings of a destination selected as a notification target are checked when `New`, `NewAsync`, or `NewDelivery` constructs the logger, before any storage is created, not at the first notification.
  - Settings that cannot create a notification writer are a configuration error at construction (`ErrInvalidNotificationTarget`); a notification is never discarded at the time it is sent.
  - The settings of destinations that are not selected as notification targets are not part of this check.
  - For a `file` target, the time display settings (time zone and date format) are checked at construction, and errors are configuration errors at construction.
- The log file of a `file` target is created at the first notification. When creation fails, each failed notification is written as one line to the standard logger, and creation is retried at the next notification.
- Notifications do not loop, even when a logger selects itself as a target (for example, `file` in the `OnErrorConfig` of a file logger).
- Even if a regular write to the same logger happens inside the processing of a notification, the internal logs of that write (delays, failures, overload, and so on) do not call the notification target again. They are written directly to the output of that logger: the same log file for file, stdout or stderr for stdout, and the same destination for Slack, OpenSearch, Loki, CloudWatch Logs, and ClickHouse. These direct writes still use the logger's log level (file also applies its overload handling), and their failures are not notified.
- Batching loggers and the Slack logger send internal log notifications after releasing their locks, and the asynchronous API (`NewAsync`) processes writes from inside a notification without waiting even when all of its slots are busy, so writing to the same logger from a notification does not hang.

## Log files

### Rotation

- If the file to rotate to cannot be opened, that write returns an error. Existing logs are kept, and rotation is retried at the next write after the problem is resolved.
- If closing the file before rotation returns an error, that write also returns an error. That file is not reused, and the next write opens a new file to complete the rotation.
- Rotation does not write diagnostic strings directly to stdout. This holds even when file is the only output, and it is the same for the regular, asynchronous, and delivery APIs.

### Storage paths and ownership

- Valid whitespace in storage paths is kept as is.
- Ownership of storage is decided on the actual paths. A configuration in which different loggers own the same directory, or a directory and its parent, at the same time is rejected.

### Case sensitivity

- Matching of log file names follows whether the file system of the storage directory is case-sensitive.
- On Windows, on case-insensitive macOS volumes, and in Linux directories confirmed to be case-insensitive, names that differ only in the case of the prefix, target, or extension are treated as the same file. This is used to choose generation numbers at rotation, to find files to compress, upload, and delete, and to identify the series of upload records (checkpoints).
- When a target name is added to a storage path such as a failure archive, whether a level already has the same name is decided by the case rule of each directory. On a case-insensitive volume, names that differ only in case are the same directory, and the target is not added twice.
- On Linux, these directories are treated as case-insensitive:
  - Directories with the casefold attribute (casefold of ext4, F2FS, and tmpfs).
  - Directories on FAT (vfat, msdos) and exFAT file systems, but only when `/proc/self/mountinfo` confirms that no case-sensitive mount option is set (`check=s` [strict] of vfat, `nocase` of msdos, `namecase=1` of the staging exFAT driver of Linux 5.4 to 5.6). When this cannot be confirmed, they are treated as case-sensitive.
- The detection only reads the file system type, attributes, and mount information; it creates no files to probe.
- A directory on ext4, F2FS, or tmpfs that cannot be read is treated as case-sensitive, because its casefold attribute cannot be checked.
- Changes of mounts or casefold attributes may take up to 2 seconds to take effect.
- SMB/CIFS, NFS, NTFS (ntfs3, ntfs-3g), FUSE (including exfat-fuse), 9p (such as Windows drives in WSL2), ZFS, and similar file systems are treated as case-sensitive, because their handling of case depends on the server or mount options and cannot be checked from outside. If such a mount is case-insensitive, do not use settings that differ only in case.
- When checking compressed log names of regular and internal logs for collisions, names that differ only in case are rejected as collisions if either the storage of the original logs or the fallback storage for failed uploads is case-insensitive.

### macOS AppleDouble files

- On file systems that cannot store extended attributes (such as FAT32 and ExFAT), macOS stores the extended attributes of a file or directory in a file named `._` + the name in the same directory (AppleDouble).
- On macOS, a `._` file whose counterpart name exists in the same directory is not treated as a log, spool, or failure archive file, and is excluded from size calculations, sending, and deletion. A `._` file without a counterpart is treated as a regular file.
- ExFAT on macOS 26 does not support renaming without replacing an existing file, so it cannot be used for the storage of spools and failure archives. Saving there fails with an error without overwriting existing files.

### Retention

- The number of kept generations (such as `HETEROLOG_MAX_LOG_FILE_BACKUPS`) is counted in generations, which are determined by the date and number in the file name.
- Uncompressed and compressed files of the same generation, including those created before the compression format was changed, count as one generation, and all files of older generations beyond the limit are deleted together.
- Files that cannot be deleted, such as a file being written or a file whose sending or fallback is not finished, are not counted.
- In the runner mode of the delivery logger, `MaxFileBackups`, `MaxFileAge`, and `MaxDirSize` given in `LifecycleRunnerSettings` are checked with the same lower bounds as the regular file logger (at least 2 generations, at least 1 day, and at least 10 MB and at least the maximum file size). Invalid values make logger construction fail even when error and debug separation is disabled.

### Compression

- Compressed files are checked for integrity before they are used for recovery. For XZ, the index and footer of each stream and the end positions of blocks are checked, so the original log is kept and can be compressed again even if a truncated compressed file remains.
- When a compressed file of the same generation as an original log remains, for the built-in formats (gzip, zstd, lz4, xz) it is decompressed to the end and compared with the original log, and the original log is deleted only when they match.
- When a compressed file is complete but its content differs, the original log is not deleted; it is moved to `.preserved` in the log directory and reported as an error, and the compressed file is treated as the file of that generation.
- Compressed files of a custom codec (`CompressionCodec`) are not compared even when the codec has a method for decompression; only the codec's own integrity check runs.
- When compressing anew with a built-in format, the temporary file is decompressed to the end before it is published and compared with the original log. If they do not match, the compressed file is not published and the original log is not deleted (it is compressed again in the next run). A custom codec still uses only its own integrity check.
- When a damaged compressed file is found while compressing right after rotation or resuming interrupted work, the original log is kept, the damaged file is deleted, and the original log is compressed again, so shutdown never keeps waiting for the same failure to be retried.
- On POSIX systems, compressed output is created with permissions no broader than those of the source file. Existing temporary outputs and additional restrictions from the umask are kept.
- On Windows, before compressed data is written, the existing private-file policy is applied, which allows access only to the current user, SYSTEM, and Administrators.

### Uploading to object storage

- When a destination is added to the object storage upload of log files, log files that existed before the destination was added are not sent to it and are not treated as sent.
- The exception is the newest file being written in the current rotation period (uncompressed): it keeps growing after the destination is added, so it is included for the new destination. Uncompressed files of past periods are excluded like other files from before the addition, even if they are the newest.
- Adding a destination is never a reason to delete files; they are deleted by the usual retention period, generation count, and size settings.
- A compressed file is deleted after upload only when it has been sent to at least one enabled destination (or, on failure, saved to every specified fallback), and every other enabled destination is confirmed to have it sent or to have excluded it as a file from before the addition.
- For object storage sending of a failure archive (fileArchive), newer generations wait until the sending of older generations completes, and are sent as soon as the completion record of an older generation advances.

### Restarts

- At startup, log file writers (regular, delivery, and the fileArchive for backend send failures) treat as used not only the files left in storage but also the generations recorded in the checkpoints of the same series (completion positions of sending and fallback, and records of sending in progress), and write from the following numbers.
- So even after a restart that follows manual recovery up to the newest file, or after only the log storage was lost, new logs never get the same name as a generation already sent, which would get them deleted without being sent or make them overwrite the same object in object storage.
- When the system clock goes back before a recorded period, writing continues in the recorded period.

## Failure handling and replay

### Spool permissions

- The spool for failed logs (OpenSearch, Loki, ClickHouse, CloudWatch Logs) creates its root and the directories below it with 0700, accessible only to the owner, and saves spool files with 0600. Additional restrictions from the umask are kept.
- Existing directories created by older versions with broader permissions also lose their group and other permissions, from the root down to the storage directory. The user's directories above the root are not changed.
- Missing directories above the root are created with 0755 (with the umask applied) as before, because they are shared with other storage such as the object storage spool.
- On Windows, newly created directories get an ACL that allows only the current user, SYSTEM, and Administrators, and spool files inherit this ACL when created. The ACLs of existing directories are not changed, so review the ACLs of spool directories created by older versions if needed.

### How failures are classified

Saving to the spool (OpenSearch, Loki, ClickHouse, CloudWatch Logs) and replaying from it follow the same rules for regular sending and spool replay:

- Temporary failures are replayed automatically: 408, 429, 5xx, known network failures such as connection failures and timeouts, sends suppressed by the circuit breaker, and Loki's 260.
- When the result cannot be confirmed (a missing or unparseable response, an interruption during sending, a send error that cannot be classified), the log is moved to `quarantine` and is not replayed automatically until its content and delivery status have been checked.
- Size errors such as HTTP 413, and rejections caused by the content of a log (such as OpenSearch mapping and parsing errors, or ClickHouse value parsing and type conversion errors), are handled by splitting a multi-entry send or resending entries one by one to find the logs that cause them; every log rejected on its own is saved to `quarantine`.
- Loki's 400 may mean that valid streams were partly accepted, so the logs sent are saved to `quarantine` without splitting.
- Problems with the destination or configuration that do not depend on the log content (rejected authentication or permissions, a missing table, index, or stream, wrong SQL or settings) make the logs undeliverable, and they are saved to `dead`.
- Events that CloudWatch Logs rejects individually as outside the accepted period are replayed if they are too far in the future, and saved to `dead` if they are too old or outside the retention period.
- Replaying can deliver a log twice if an earlier attempt was accepted.
- When the circuit breaker tries whether it can resume sending, it lifts the suppression only when a response from the destination is confirmed. If the client returns neither a response nor an error, the result is unknown and sending stays suppressed.
- When a spool record cannot be restored or validated at replay, it is moved to `quarantine`. The error can be identified by its kind (sentinel) and field name, but the stored value is not included in errors that can be retrieved with `errors.As` (causes or fields).

### Retry-After

- When an OpenSearch, Loki, ClickHouse, or CloudWatch Logs destination returns `Retry-After` with a retryable failure, the spool does not replay to that destination until the given time has passed since the response was received.
- The value, whether received during spool replay or regular sending, is recorded as a waiting deadline for the whole destination, and none of that destination's logs, including older logs without a deadline, are sent before it.
- A value received during regular sending is recorded when the failed log is saved to the spool (including when the log is not saved to the spool because of a `drop` or `degrade` on overload), so a running runner applies it from its next cycle.
- A value received during spool replay also stops the logs of the same replay that were not sent yet.
- A new value does not shorten a later deadline already recorded.
- Replay to other destinations, deletion of spool files by retention period and size, and retries of post-processing continue.
- The maximum wait is 1 hour by default and can be changed with `Worker.MaxRetryAfter` for each backend (environment variables `*_SPOOL_RUNNER_MAX_RETRY_AFTER_SECOND`, such as `HETEROLOG_OPENSEARCH_SPOOL_RUNNER_MAX_RETRY_AFTER_SECOND`). A longer value is shortened to the maximum, with a warning to the internal log.
- If the value is shorter than the replay cycle, replay happens at the usual cycle.
- Client retries within a single send wait up to `MaxDelay`, for both regular sending and spool replay.

How the deadline survives restarts:

- The deadline is saved as `.retry-after/<backend name>.json` (for example, `opensearch.json`) in the spool directory (`<SpoolPath>/<Target>/spool/<Dir>`) and loaded before the first replay after a restart, so the destination is not replayed before the deadline regardless of the order in which logs are loaded.
- The given time is also saved in the logs waiting for replay. If the deadline could not be saved, it is restored when such a log is loaded.
- A log or deadline whose saved time is further away than the maximum at loading is shortened to the maximum, with a warning to the internal log. The shortened deadline is saved too, so restarts do not extend the wait.
- Records past their deadline are deleted (a record shortened to the maximum is kept until its original time passes, to show that it was shortened).
- Temporary files left by a stop in the middle of saving (`.<backend name>.json.<digits>.tmp`) are deleted at the next load.
- When a record cannot be read or its content is broken, a warning is written to the internal log, replay continues as if there were no record, and the next `Retry-After` replaces it.
- When saving a record fails, a warning is written to the internal log (only once while failures continue), and the process waits until the deadline while retrying the save until the deadline arrives. The retry interval starts at the replay cycle and doubles at each failure, up to 1 minute. If the process restarts before the record could be saved, the deadline may not be carried over.

### Compressed log uploads

- The spool that resends compressed log files to object storage sends the file again to the same destination and object name as when it was saved. So even for send errors that cannot be classified and for panics in the upload, the file is not moved to `quarantine`; it is returned to waiting and resent automatically. It is moved to `dead` only when the destination's response shows a permanent rejection.
- When the destination of compressed log files returns `Retry-After`, only the files for that destination wait until the given time; resending to other destinations continues.
- A value received with a temporary failure by a regular upload before the file is saved to the spool is also recorded as that destination's waiting deadline when the file is saved, so a running resend process applies it from its next cycle and does not send files to that destination before the deadline.
- A new value does not shorten a later deadline already recorded.
- The maximum wait is 1 hour by default and can be changed with `HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_MAX_RETRY_AFTER_SECOND` (the setting of the same name starting with `DELIVERY_` for the delivery logger). A longer value is shortened to the maximum, with a warning.
- The deadline is saved as `.retry-after/<destination ID>.json` in the spool directory of the destination (`<SpoolPath>/<Target>/spool/objectstorage/<service>/<logger contract>/<destination ID>/.retry-after/<destination ID>.json`) and loaded before the first send to that destination after a restart, so files for that destination are not sent before the deadline.
- The destination ID is used for the names of the per-destination spool and file archive directories and of `.retry-after/<destination ID>.json`, so it must start with a lowercase letter or digit, consist only of lowercase letters, digits, hyphens, and underscores, be at most 128 characters long, and not be a reserved Windows device name (`con`, `prn`, `aux`, `nul`, `com0` to `com9`, `lpt0` to `lpt9`). S3 destination IDs (`s3-` followed by 64 hexadecimal digits of SHA-256) always follow this rule.
- In a configuration that uses storage for failed uploads (a spool or a file archive), a destination ID that breaks the rule makes logger construction return a configuration error without creating any storage.
- A time further away than the maximum at loading is shortened to the maximum and saved again, with a warning, so restarts do not extend the wait.
- Records past their deadline and temporary files left by a stop in the middle of saving (`.<destination ID>.json.<digits>.tmp`) are deleted.
- When a record cannot be read or its content is broken, a warning is reported, resending continues as if there were no record, and the next `Retry-After` replaces it.
- When saving a record fails, a warning is reported (only once while failures continue), and the process waits until the deadline while retrying the save until the deadline arrives. The retry interval starts at the resend cycle and doubles at each failure, up to 1 minute. If the process restarts before the record could be saved, the deadline may not be carried over.

### Durability and cancellation

- Restoring the OpenSearch and ClickHouse spools keeps the precision of JSON numbers. Large integers and high-precision decimals are not converted to `float64` for replay, and saved document IDs and deduplication tokens are kept.
- When appending to a fileArchive fails in the middle, the file is rolled back to the position before the append. While it cannot be recovered, later saves and rotation stop with an error, and recovery is retried at the next save. If the end of an existing file is incomplete at a restart, opening it is refused with its content kept, so the file needs to be checked and repaired.
- When regular sending to OpenSearch, Loki, ClickHouse, or CloudWatch Logs times out or is canceled, saving the failed logs is still attempted with its own deadline.
- For both regular sending and spool replay, logs whose result became unknown because the caller's deadline or cancellation interrupted the send go to `quarantine`, logs not sent yet are kept for replay, and logs whose success is confirmed are not added again. When an explicit response was received, its result is kept.
- With CloudWatch Logs, even if saving a failed batch fails in the middle, later sends and saves continue while the send context is valid, and the errors of each step are returned together.
- The status and `Retry-After` of an HTTP response are not overridden by words in the response body that look like a network failure.
- A `Retry-After` too large to be represented as a wait does not cause an immediate resend; it is treated as the maximum value, and then wait limits such as `MaxDelay` apply.
- Connection resets, aborts, and refusals are detected by error number, not by error messages that differ between operating systems, so they are retried as temporary disconnections on Windows too.
- Backend-specific error types are classified by the retry rules of each service.
- When a send is canceled while waiting for the next attempt after an HTTP rejection, both the confirmed response and the cause of the cancellation are kept. Temporary rejections such as 503 are saved for replay, and logs with an unknown result, such as those interrupted during sending, go to quarantine.
- When an OpenSearch ingest pipeline intentionally drops a log and responds with `noop`, the log is treated as processed. The other logs of the same batch are judged by their own responses.
- When deleting the original `processing` file fails after saving to `done`, `dead`, or `quarantine`, the saved content is compared at the next start, and only the original file is deleted if every entry is confirmed saved in a single state. A partial save, a split across several states, or mismatching content keeps the file, returns an error, and stops automatic replay. The saved files needed for this check are protected from deletion by retention period and size while their original file remains.
- Saving replay results is complete only after the file and its storage directory are synced. If syncing fails after publishing, the saved file and the original `processing` file are kept, and at the retry the content is compared and the sync is redone. Existing results are not overwritten, and the time of the first confirmation is kept.
- When post-processing of the spool fails, the `RetryDelay` of the state transition is counted from when the failure was found, so the configured wait before the next retry is kept even when a file operation took a long time.
- The spool for regular logs reports a successful save only after the file and its storage directory are confirmed to be synced. If syncing fails after publishing, the saved file is kept. Saving the same log again after this error stores it in another file to avoid losing it, so it may be duplicated.
- When the storage directory of a spool, fileArchive, or checkpoint is newly created, success is reported only after the parent directory is confirmed to be synced. If syncing the parent fails, `ErrDurabilityUnconfirmed` is returned, and the sync is retried each time the same process prepares storage that depends on that parent. Until it succeeds, the same error is returned even for directories already created.
- The spool's size statistics are updated even when `StatsScanInterval` is 0 or less. The first scan runs synchronously, and later results of asynchronous rescans are used for the size decision. `MaxCallsSinceScan=0` disables only the update based on the number of calls.

### Timeout budget

- The timeout budget check of `must_fit` includes waits for `Retry-After` up to `MaxDelay`, also for regular retries of Loki and ClickHouse.
- When timeout and retry settings of loggers and clients are omitted in the environment configuration, the defaults are the same as the values in `.env.example` and satisfy the default `must_fit` (18 seconds for the regular and asynchronous loggers, 12 seconds for the delivery logger, 1 second for the OpenSearch client, and 2 seconds for the Loki, ClickHouse, and CloudWatch Logs clients).
- The recovery CLI, which reads the environment configuration, checks the whole configuration including this budget before starting recovery, and exits with a configuration error if it is not satisfied.

## Sending traces

`NewTelemetry` records spans with its `Tracer` and sends them, together with spans from the OpenTelemetry SDK, to the destinations in `TelemetryConfig.Traces.Exporters`: an OTLP/HTTP receiver (`otlp`), such as the OpenTelemetry Collector or Grafana Tempo, a file (`file`), and stdout (`stdout`). Without destinations, its `Tracer` records nothing. Start from `DefaultTelemetryConfig()`, which fills every setting except the destinations; `TelemetryConfig.Validate` lists the invalid settings, and `NewTelemetry` returns the same errors.

### Spans

- A span becomes a child of the span context in the context passed to `StartSpan`: that of a span the `Tracer` started, or one stored with `pkg/observer.ContextWithSpanContext`. It follows the sampling decision of its parent. A span without a parent starts a new trace, which `SamplingRatio` decides to record or not from the trace ID, as the `TraceIDRatioBased` sampler of the OpenTelemetry SDK does, so services that share a ratio agree.
- A span that is not recorded still puts its span context in the returned context, with `Sampled` false, so its children and other services follow the decision.
- `RecordError` adds an `exception` event with `exception.type` and `exception.message` and sets the status to `StatusError`. `SetStatus(StatusOK, ...)` is final: later calls do not change it.
- `pkg/observer.StartSpanWithOptions` passes a span kind, such as `SpanKindServer`, and links to other spans. Spans started with `StartSpan` are `SpanKindInternal`.
- A span keeps up to 128 attributes, events, and links, and each event and link up to 128 attributes. The rest are dropped, and their numbers are sent as the dropped counts of OTLP. A later attribute with the same key replaces the value.
- Calls after `End` have no effect, and the methods of a span may be called concurrently.
- The spans of the `Tracer` have the instrumentation scope `github.com/go-jappe/heterolog` and a resource made from `TelemetryConfig.Resource`, with the same attribute names as the OTel log format: `service.name` (`unknown_service:` followed by the executable name when empty), `service.version`, `host.name`, `os.type`, `deployment.environment`, `heterolog.runtime.type`, `service.instance.id`, `cloud.provider`, and `cloud.region`. Empty values are left out.

### Continuing a trace across services

`pkg/observer` reads and writes the W3C `traceparent` header:

```go
// Incoming request: continue the caller's trace.
if sc, err := observer.ParseTraceParent(r.Header.Get("traceparent")); err == nil {
    ctx = observer.ContextWithSpanContext(ctx, sc)
}
ctx, span := observer.StartSpanWithOptions(ctx, tel.Tracer(), "GET /items",
    observer.StartOptions{Kind: observer.SpanKindServer})
defer span.End()

// Outgoing request: pass the current span on.
req.Header.Set("traceparent", observer.SpanContextFromContext(ctx).TraceParent())
```

- `ParseTraceParent` accepts the fields of version `00` from later versions too, and rejects uppercase digits and all-zero IDs. Its error does not include the header value.
- The `tracestate` header is not read or written.

### Spans from the OpenTelemetry SDK

`otelexport.NewSpanExporter(tel)` returns an `sdktrace.SpanExporter` that queues the spans of the SDK in the `Telemetry`, which sends them with its own spans. It is in the separate package `github.com/go-jappe/heterolog/otelexport`, so programs that do not use the SDK do not link it.

- The SDK samples its spans; `SamplingRatio` applies only to the `Tracer` of the `Telemetry`.
- The spans keep the resource and instrumentation scope of the SDK. A span with an empty resource is sent with the resource of the `Telemetry`.
- `ExportSpans` returns without waiting for the network. `Shutdown` of the exporter only stops accepting spans. Shut down the `TracerProvider` before the `Telemetry`, so that the spans the SDK still holds reach the `Telemetry` before its `Shutdown` sends the last batches.

### Delivery

- Each destination has its own queue of `QueueSize` spans and its own goroutine, so a slow destination does not delay the others. `Span.End` and `ExportSpans` never wait: a span that does not fit in the queue of a destination is dropped for that destination.
- A destination sends a batch when `BatchSize` spans are waiting, or `ExportInterval` after its previous send. While it is sending, spans wait in the queue.
- `otlp` posts the batch in protobuf (`Content-Type: application/x-protobuf`), compressed with gzip unless `Compression` is `none`, to `OTLP.Endpoint` exactly as given (`/v1/traces` is not added), with `OTLP.Headers`. `OTLP.Timeout` limits each request. Redirects are not followed and are not retried, as for the log destinations.
- Failed requests are retried up to `OTLP.Retries` times by the rules of the [OTLP specification](https://opentelemetry.io/docs/specs/otlp/), which differ from those of the log destinations: only the statuses 429, 502, 503, and 504 and failures without a response, such as a refused connection, a broken connection, or a timeout, are retried. The specification forbids retrying other statuses, such as 400, 408, or 500.
- The wait before a retry doubles from `OTLP.RetryBaseDelay` with each retry, up to `OTLP.RetryMaxDelay`, and a random wait between half of it and all of it is chosen, so that many processes do not retry at the same moment. When the response has a `Retry-After`, it is used instead; when it is longer than `OTLP.RetryMaxDelay`, the request is not retried, because the specification asks clients to follow it.
- When the receiver rejects a request as too large (413), its spans are split in half and each half is sent in its own request, down to single spans. A single span that is still too large fails.
- When a request fails after its retries for a reason that would allow a retry, the remaining requests of the same batch, such as the other half after a split, are not sent and fail in the same way.
- A response that reports a partial success counts its rejected spans as failed and is not retried.
- With `OTLP.CircuitBreaker`, the same circuit breaker as that of the log destinations stops sending for a while after requests keep failing temporarily: when at least 3 failures in a row, each within 15 seconds of the previous one, have lasted a minute, requests fail without being sent for 30 seconds, and then one request is sent to check whether the receiver has recovered. Its metrics have the `backend` `otlp_traces`.
- `file` and `stdout` write each batch as one line of OTLP JSON: trace and span IDs in hexadecimal, enumerations as numbers, and 64-bit integers and times as strings. `file` appends to `File.Path`, creating missing directories; the file is not rotated or removed.
- Spans that `otlp` cannot send after the retries go to `OTLP.FailureStore` (see [Failed spans](#failed-spans)). Without it, and for `file` and `stdout`, they are dropped.
- The errors of the delivery are passed to the function given with `WithTelemetryErrorHandler`, or written as one line each to the standard logger without it: spans that could not be sent and are not resent later (dropped, or kept without a resend), and the errors and warnings of the failure store. Spans dropped because a queue was full are reported in one error per destination at most once per `ExportInterval`, and spans that ended after `Shutdown` started only the first time. The function may be called concurrently and delays the delivery to the destination until it returns. A panic in it is recovered and written to the standard logger. Pass `otel.Handle` to send the errors to the error handler of the OpenTelemetry SDK.
- The delivery does not create spans for its own sending, which would in turn be sent. The metrics below report it instead, recorded with the `Meter` passed to `NewTelemetry` with `WithObservers`; the `Tracer` of that option is not used.
- `Shutdown(ctx)` stops accepting spans, sends the queued spans, and closes the destinations. When `ctx` ends first, it returns `ctx.Err()` and the sending continues in the background; a later `Shutdown` waits for the same cleanup. Spans that end after `Shutdown` started are dropped. Close the loggers that use the `Tracer` before the `Telemetry`, so that their last spans are sent.

### Failed spans

`OTLP.FailureStore` keeps the spans that `otlp` could not send, with the failure stores of the log destinations: `Store` is a `pkg/logger/durable` `FailureStoreConfig`, as for a log destination, and `Replay` and `ReplaySupervisor` configure the worker that resends the spool. `DefaultTelemetryConfig` leaves `Store.DurableType` empty, which keeps nothing, and fills `Replay` and `ReplaySupervisor`; the environment variables select `spool` by default.

- Each span is kept as one record of OTLP JSON with its resource and instrumentation scope, so a record in the spool, dead, quarantine, or the failure archive can be read as it is. The records have the backend `otlp_traces`, and so do the metrics of the spool, the failure archive, and the circuit breaker.
- Spans that failed for a reason that allows a resend (429, 502, 503, 504, a failure without a response, or an open circuit breaker) go to the active spool and are resent by the spool worker, also after a restart. The worker decodes the records and sends them again with the same requests, retries, splitting, and circuit breaker as regular sends. A `Retry-After` longer than `OTLP.RetryMaxDelay` is kept for the whole destination, up to `Replay.Worker.MaxRetryAfter`, as for the log destinations.
- Unlike logs, spans whose outcome is unknown, such as after a broken connection, are resent instead of going to quarantine, because duplicates are preferred over losses. A resent span can therefore arrive twice; it has the same trace ID and span ID.
- Spans that the receiver rejected with another status are kept as dead, and the spans of a request that reported a partial success as quarantine, because it is not known which of them were accepted. Neither is resent, and both are reported to the error handler.
- With `DurableTypeFileArchive`, the spans are appended to a failure archive, with the same rotation, compression, and retention as for logs. Its upload to object storage is not supported yet, and `Validate` rejects it. With `DurableTypeNone` and `EnableDegrade`, the spans are written to stdout.
- The overload handling of the stores (free disk space, the size of the active spool, and memory pressure, with `drop` or `degrade`) is the same as for logs.
- The internal log notification targets of the stores (`HandlerConfig`) are not used. Their errors and warnings go to the function given with `WithTelemetryErrorHandler`.
- The spool and the failure archive are directories of their own. The same directory cannot be used by a log destination or by another `Telemetry` in the same process at the same time.
- The delivery is guaranteed only for spans that heterolog holds: those of its `Tracer`, and those of the OpenTelemetry SDK from the moment `ExportSpans` of `otelexport` accepts them. Spans that the SDK drops or fails to send by itself, such as with its own exporter, are handled by the SDK, as are its errors. Spans dropped because a queue was full, or because they ended after `Shutdown` started, are not kept.
- `Shutdown` waits for the spans being stored, and then stops the spool worker after its current pass. Spans left in the spool are sent by the next `Telemetry` that uses the same directory.

### Environment variables

`env.TelemetryConfig()` and `env.NewTelemetry(ctx)` use these keys, all listed in [.env.example](../.env.example):

| Key | Setting | Default |
|---|---|---|
| `HETEROLOG_ENABLE_TRACES` | Whether to send traces. When `false`, the other keys are not read. | `false` |
| `HETEROLOG_TRACES_TYPE` | `Exporters`, separated by commas. | `otlp` |
| `HETEROLOG_TRACES_SAMPLING_RATIO` | `SamplingRatio` | `1` |
| `HETEROLOG_TRACES_QUEUE_SIZE`, `HETEROLOG_TRACES_BATCH_SIZE` | `QueueSize`, `BatchSize` | `2048`, `512` |
| `HETEROLOG_TRACES_EXPORT_INTERVAL_MILLISECOND` | `ExportInterval` | `5000` |
| `HETEROLOG_TRACES_OTLP_ENDPOINT` | `OTLP.Endpoint` | `http://localhost:4318/v1/traces` |
| `HETEROLOG_TRACES_OTLP_HEADERS` | `OTLP.Headers`, as a JSON object | none |
| `HETEROLOG_TRACES_OTLP_COMPRESSION` | `OTLP.Compression` | `gzip` |
| `HETEROLOG_TRACES_OTLP_TIMEOUT_SECOND` | `OTLP.Timeout` | `10` |
| `HETEROLOG_TRACES_OTLP_RETRIES` | `OTLP.Retries` | `3` |
| `HETEROLOG_TRACES_OTLP_RETRY_BASE_DELAY_MILLISECOND`, `HETEROLOG_TRACES_OTLP_RETRY_MAX_DELAY_MILLISECOND` | `OTLP.RetryBaseDelay`, `OTLP.RetryMaxDelay` | `1000`, `5000` |
| `HETEROLOG_ENABLE_TRACES_OTLP_CIRCUIT_BREAKER` | `OTLP.CircuitBreaker` | `false` |
| `HETEROLOG_TRACES_OTLP_DURABLE_TYPES` and the other failure store keys with `TRACES_OTLP` in place of the destination name, such as `HETEROLOG_TRACES_OTLP_FAILURE_SPOOL_DIR` and `HETEROLOG_TRACES_OTLP_SPOOL_RUNNER_INTERVAL_MILLISECOND` | `OTLP.FailureStore`. They have the same meaning and defaults as the keys of the log destinations, such as `HETEROLOG_LOKI_*`, except that there are no `*_HANDLER` keys and no upload keys for the failure archive. The directories are `traces_otlp` (spool) and `traces_otlp_failure_archives`, resolved like those of the log destinations, and the restarts of the spool worker follow `HETEROLOG_MAX_RESTART_SPOOL_TIMES`. | `spool` |
| `HETEROLOG_TRACES_FILE_DIR` | The directory of `File.Path`, which is `spans.jsonl` in it. Like the log directories, it is resolved under `HETEROLOG_SYSTEM_LOG_DIR` and `HETEROLOG_TARGET`. | `/traces` |

- The resource comes from the keys the loggers use: `HETEROLOG_APP_NAME` (`service.name`), `HETEROLOG_APP_ENV`, `HETEROLOG_RUNTIME_TYPE`, `HETEROLOG_INSTANCE_ID`, `HETEROLOG_CLOUD_PROVIDER`, and `HETEROLOG_CLOUD_PROVIDER_REGION`, with the host name and OS of the process.
- `LoadEnvConfig` checks the trace settings when `HETEROLOG_ENABLE_TRACES` is `true` and reports the problems under `HETEROLOG_TRACES_*`, as `pkg/telemetry` `FieldError`s. The values of the endpoint and the headers, which can contain credentials, are not shown.

## Metrics and traces

Pass a `Tracer` and a `Meter` with `WithObservers` to receive the metrics and spans below. Without them, nothing is recorded. The `heterolog_trace_*` metrics are those of [Sending traces](#sending-traces), recorded with the `Meter` passed to `NewTelemetry`.

### Metrics

- Names follow the Prometheus conventions: they start with `heterolog_`, counters end with `_total`, and units are part of the name. Durations are in seconds.
- The `backend` attribute is the name of a `pkg/logger.LogOutputDestination`, such as `opensearch` or `slack`.
- Spool replay sends through the same kind of retry client and circuit breaker as regular sending, so the retry and circuit breaker metrics include its requests. `heterolog_spool_replay_records_total` counts the replayed records. The other backend metrics count only regular sending.
- Internal log notifications are not counted in `heterolog_backend_records_total` or in the flush metrics. `heterolog_internal_logs_total` counts them.

| Metric | Type | Attributes | Meaning |
|---|---|---|---|
| `heterolog_backend_records_total` | Counter | `backend`, `outcome` | Records sent to OpenSearch, Loki, ClickHouse, CloudWatch Logs, or Slack, by result: `success`, or the failure classes `retryable`, `dead`, and `quarantine` described in [How failures are classified](#how-failures-are-classified). Slack keeps the records it could not send for its next batch and counts them as `retryable` at each try, and counts records it discards because its buffer is full as `dropped`. A failed Slack delivery is `dead`. The file logger counts each write as `success` or `failure`; entries it discards with the overload policy `drop` are counted in `heterolog_file_dropped_entries_total` instead. |
| `heterolog_backend_request_duration_seconds` | Histogram | `backend`, `outcome` (`success`, `failure`) | Duration of one request, including the retries of the retry client and the waits between them. A response that rejects only some records is a `success`; the records are counted in `heterolog_backend_records_total`. |
| `heterolog_backend_flush_records` | Histogram | `backend` | Records in one buffered batch. |
| `heterolog_backend_flush_bytes` | Histogram | `backend` | Bytes of one buffered batch, as counted against `BufferFlushBytes`. Slack does not record it. |
| `heterolog_backend_retries_total` | Counter | `backend`, `reason` | Retries by the retry client, by the reason for the retry, such as `http_throttling` or `net_timeout`. |
| `heterolog_circuit_breaker_rejected_requests_total` | Counter | `backend` | Requests that the circuit breaker rejected without sending. |
| `heterolog_circuit_breaker_transitions_total` | Counter | `backend`, `state` (`open`, `closed`) | Times the circuit breaker opened, and times it closed after a probe confirmed recovery. |
| `heterolog_failure_records_total` | Counter | `backend`, `action` | Records that failed to send, by what happened to them: `spooled`, `archived` (to the failure archive), `degraded` (written to stdout), `dropped` (by the overload policy `drop`), or `failed` (could not be stored or written). When several durable types are configured, each one counts the record. |
| `heterolog_async_backend_failures_total` | Counter | `backend`, `reason` (`error`, `timeout`, `panic`) | Entries that the asynchronous API (`NewAsync`) passed to a destination and that the destination returned an error for, did not finish within `Timeout`, or panicked on. The asynchronous API does not return these failures to the caller. An entry that the destination discards with its overload policy is an `error`. |
| `heterolog_async_rejected_entries_total` | Counter | `reason` (`invalid`, `closed`) | Entries that the asynchronous API discarded without passing them to any destination: `invalid` entries, such as one larger than `MaxLogEntryBytes`, and entries logged after `Close` started. |
| `heterolog_object_storage_uploads_total` | Counter | `target` (such as `s3`), `outcome` | Uploads of a file, such as a compressed log file or a failure archive, to one object storage destination, by result: `success`, `retryable` (kept to be uploaded again later), or `permanent`. Uploads replayed from the spool are included. |
| `heterolog_object_storage_upload_duration_seconds` | Histogram | `target`, `outcome` | Duration of one upload to one destination. |
| `heterolog_memory_pressure_ratio` | Gauge | `backend` | Memory usage ratio (0 to 1) last read by the memory pressure monitor that the spool of the destination starts for `UseMemoryPressureForOverload`, recorded only when the reading is reliable. A monitor passed with `WithMemoryPressureMonitor` is owned by the caller and is not recorded. |
| `heterolog_memory_pressure_critical` | Gauge | `backend` | 1 when the last evaluation of the same monitor found memory critical, and 0 otherwise. |
| `heterolog_spool_replay_records_total` | Counter | `backend`, `outcome` | Records read back from the spool and sent again, by result: `success`, `retryable` (returned to the spool to be sent again later), `dead`, or `quarantine`. A response that cannot be matched to the records quarantines the whole file, and all its records are counted as `quarantine`. |
| `heterolog_file_dropped_entries_total` | Counter | none | Entries that the file logger discarded with the overload policy `drop`. |
| `heterolog_trace_spans_total` | Counter | `exporter` (`otlp`, `file`, `stdout`), `outcome` | Spans that a destination of `NewTelemetry` handled, by result: `success`; `retryable`, `dead`, or `quarantine` (not sent, and kept in the failure store to be resent, or without a resend); `failure` (not sent and not kept); or `dropped` (did not fit in the queue, or ended after `Shutdown` started). For a partial success, only the rejected spans are counted as `quarantine`, although the whole request is kept. |
| `heterolog_trace_export_duration_seconds` | Histogram | `exporter`, `outcome` (`success`, `failure`) | Duration of sending one batch of spans, including the retries and the waits between them. A batch is a `failure` when none of its spans was sent. |
| `heterolog_trace_export_retries_total` | Counter | `exporter`, `reason` | Retries of the `otlp` destination, by the reason, such as `http_status` or `net_timeout`. Splitting a request that was too large counts as `request_too_large`. |
| `heterolog_internal_logs_total` | Counter | `level` (`error`, `warn`, `info`, `debug`) | Internal logs passed to the internal log notification targets (`OnErrorConfig` and the like), such as a failed write or a slow request, by level. See [Internal log notifications](#internal-log-notifications). |

### Spans

- Span names start with `heterolog.`, followed by the destination (or a part of the logger, such as `spool` or `failurearchive`) and the operation. Span attributes start with `heterolog.`.
- When the operation fails, the span records the error.
- A span is a child of the span in the context of the operation. A send started by a logging call, for example when a buffer reaches its threshold, is part of the caller's trace.
- The file logger does not create a span for each write. `heterolog_backend_records_total` counts its writes.

| Span | Operation | Attributes |
|---|---|---|
| `heterolog.objectstorage.upload` | Uploading one file to one object storage destination. | `heterolog.target`, `heterolog.destination` (the destination ID, such as `s3-<SHA-256>`) |
| `heterolog.spool.replay` | Sending records read back from the spool once. The tries of the retry client are its children. | `heterolog.backend`, `heterolog.records` |
| `heterolog.<destination>.create` | Constructing the logger of a destination (`file`, `stdout`, `slack`, `opensearch`, `loki`, `cloudwatchlogs`, `clickhouse`). | |
| `heterolog.opensearch.flush`, `heterolog.loki.flush`, `heterolog.cloudwatchlogs.flush`, `heterolog.clickhouse.flush`, `heterolog.slack.send` | Sending one buffered batch. | `heterolog.records`, `heterolog.bytes` (not Slack) |
| `heterolog.opensearch.bulk`, `heterolog.opensearch.index`, `heterolog.loki.push`, `heterolog.cloudwatchlogs.put_log_events`, `heterolog.clickhouse.insert`, `heterolog.slack.post` | One request, including the retries of the retry client. | `heterolog.records` |
| The request name followed by `.attempt`, such as `heterolog.loki.push.attempt` | One try by the retry client, without the wait before it. | `heterolog.attempt` (1 for the first try) |
| `heterolog.file.compress`, `heterolog.file.purge.age`, `heterolog.file.purge.generation`, `heterolog.file.purge.dirsize` | Compressing a rotated log file, and deleting log files by age, generation count, and directory size. | `heterolog.compress.format` |
| `heterolog.failurearchive.write`, `heterolog.failurearchive.write.retryable`, `heterolog.failurearchive.write.permanent`, `heterolog.failurearchive.compress`, `heterolog.failurearchive.purge.age`, `heterolog.failurearchive.purge.generation`, `heterolog.failurearchive.purge.dirsize`, `heterolog.failurearchive.delegate`, `heterolog.failurearchive.create` | Saving failed records to the failure archive, maintaining its files, notifying its internal logs, and constructing it. | |
| `heterolog.fallback.slack.create` | Constructing the Slack logger for internal log notifications. | |
| `heterolog.internal_log.notify` | Passing one internal log to one notification target. | `heterolog.destination` (such as `file` or `slack`), `heterolog.level` |

Events added to the span of the operation:

- `heterolog.<destination>.retry_decision` (`opensearch`, `loki`, `cloudwatchlogs`, `clickhouse`): the retry decision of the retry client after a failed try (the reason, the attempt, and the HTTP status), and the decision of the circuit breaker when it records a failure or rejects a request.
- `heterolog.failure.overload.dropped`: failed records discarded by the overload policy `drop`, with the number of records of each failure class. The span also gets the attribute `heterolog.failure.overload.drop_occurred`.
- `heterolog.file.overload.dropped`: an entry discarded by the file logger with the overload policy `drop`.

## Destination notes

### Slack

- `MaxRestThreshold` is the idle time between arriving logs. When a log arrives after this time has passed, an immediate send is scheduled together with the waiting logs. If a notification is being sent, it waits for that send to complete.
- The `HTTPClient` of `SlackLoggerConfig` and `DeliverySlackLoggerConfig` in `pkg/logger` accepts the `*http.Client` used to call the Slack API (for example, to use a proxy or a custom transport). When it is nil, the default transport of `net/http` is used.
  - The given client is copied at construction, keeping `Transport` and `Jar`. The original client is not modified, and changes after construction are not reflected. Redirects are not followed, whatever its `CheckRedirect` (see [Endpoint URLs and redirects](#endpoint-urls-and-redirects)).
  - The timeout is the `Timeout` of each configuration (it must be positive), which takes precedence over the `Timeout` of the original client.
  - The same client is used for internal logs sent with the same Slack settings.
  - It cannot be set from the environment configuration, so Slack output enabled there uses the default transport.
- The Slack backend reports `DeliveryRejected` when the delivery context ended before sending, and `DeliveryUnconfirmed` for network failures after sending or incomplete responses.
- The public delivery API uses a delivery deadline that is independent of the caller's cancellation.
- Success is decided by validating the whole response body.
- Slack has no durable storage or quarantine for failed logs, so if you need to keep logs with an unconfirmed delivery result, handle that in the caller.
- For regular and asynchronous Slack sending, batches whose response body could not be validated, and whose result is therefore unknown, are resent as well. Duplicates are preferred over losses, so the same notification may be posted twice.

### CloudWatch Logs

- Stream names after routing (splitting into streams) are checked for the regular and internal log settings actually used. Base settings not selected as outputs, and base settings replaced by settings dedicated to internal logs, are excluded from the check.
- The size limit is decided by the message length after JSON encoding plus the per-event overhead (26 bytes), and a single message is sent up to 1,048,550 bytes. A log that exceeds the limit is excluded from sending without stopping the other logs of the same batch.
- When the endpoint URL is omitted, the standard endpoint of the region is used. China regions use `amazonaws.com.cn`. An explicit URL takes precedence.
- With automatic stream creation, when a send is rejected because the stream does not exist, logs that were not accepted are saved for replay even if creation is canceled. A permanent rejection by the create API is treated as undeliverable, and if the result of resending after creation is unknown, the logs go to quarantine.
- When obtaining credentials or signing fails and the HTTP request of the logs is confirmed not to have been sent, the logs are kept for replay. The original cause can be retrieved with `errors.Is` and `errors.As`.

### Loki

- The line limit is decided by the UTF-8 byte length of the string as sent. Invalid UTF-8 is replaced as Go's JSON encoding does, and splitting and exclusion use the length after replacement.

### OpenSearch

- When sending a single document, the index name is encoded as a single URL path element. Special characters in date math such as `<logs-{now/d}>` and in time zone specifications are kept.

### Endpoint URLs and redirects

- When the trailing `/` of a Loki or CloudWatch Logs endpoint URL is removed, escaped forms such as `%2F` are kept; an encoded `/` is not turned into a path separator.
- No output follows redirects. Sending to Loki, ClickHouse, OpenSearch, CloudWatch Logs, Slack, and VictoriaMetrics and uploading to S3 send each request only once and return a redirect, including 301, 302, 303, 307, and 308, as an HTTP status error: following it would turn the request into a GET without a body, or send the logs to a URL that is not configured. The redirect settings of a given HTTP client are not used and not changed.
- A redirect is treated as a permanent failure caused by a wrong destination or setting and is not resent automatically; compressed logs, for example, are kept in the fileArchive `permanent` or the spool `dead`. Only the `307 TemporaryRedirect` that S3 returns while the DNS of a bucket is being updated is treated as a temporary failure and resent.

### ClickHouse

- `InsertDeduplicationToken` of `pkg/clickhouse` is sent as is, including leading and trailing whitespace. Only an empty string means it is not set. Use the same token so that a resend is treated as the same INSERT, and use different values for different INSERTs.

### S3 and object storage

- The object name of `pkg/objectstorage/uploader.NewUploadInput` keeps leading and trailing whitespace; only an empty name is rejected.
- Upload to S3 supports empty files.
- The S3 region is signed with only its leading and trailing whitespace removed; its case is not changed. Destination IDs treat regions that differ only in case as the same destination.
- An S3 endpoint cannot contain a query (including an empty query that is just `?`) or a fragment.
- S3-compatible services work with the same `HETEROLOG_OBJECT_STORAGE_S3_*` settings if they accept AWS Signature Version 4 with an unsigned payload: uploads are PUT requests signed with `X-Amz-Content-Sha256: UNSIGNED-PAYLOAD`. Set `HETEROLOG_OBJECT_STORAGE_S3_ENDPOINT` to the endpoint of the service and `HETEROLOG_OBJECT_STORAGE_S3_REGION` to the region that the service expects in the signature.
- Backblaze B2 works this way. Use the S3 endpoint shown for the bucket with `https://`, and the region in its name (such as `us-west-004`). The access key ID is the keyID of an application key that can write to the bucket, the secret access key is its applicationKey, and `HETEROLOG_OBJECT_STORAGE_S3_SESSION_TOKEN` stays empty. The master application key cannot be used with the S3-compatible API of B2. Both virtual-hosted-style and path-style requests work, so `HETEROLOG_ENABLE_OBJECT_STORAGE_S3_FORCE_PATH_STYLE` can stay `false`. Set the bucket in the `*_OBJECT_STORAGE_S3_BUCKET` key of each upload. For example:

  ```sh
  HETEROLOG_OBJECT_STORAGE_S3_ENDPOINT=https://s3.us-west-004.backblazeb2.com
  HETEROLOG_OBJECT_STORAGE_S3_REGION=us-west-004
  HETEROLOG_OBJECT_STORAGE_S3_ACCESS_KEY_ID=<keyID>
  HETEROLOG_OBJECT_STORAGE_S3_SECRET_ACCESS_KEY=<applicationKey>
  HETEROLOG_LOG_FILE_OBJECT_STORAGE_S3_BUCKET=<bucket>
  ```

### VictoriaMetrics

- `SendAsync` of `pkg/victoriametrics` copies the metrics and labels it accepts, so the inputs can be reused after it returns.
- The settings of the client passed to `WithHTTPClient` are not changed. `WithTimeout` applies to each HTTP request, for both synchronous and asynchronous sending, and each resend uses the same deadline again. 0 means no deadline.

## Log recovery CLI

In addition to the library, a CLI that recovers the log files of a stopped application is included.

```sh
go build -o log-recovery ./cmd/log-recovery
./log-recovery --help
./log-recovery --target my-service --application-stopped --env-file /path/to/logging.env
```

- `--target` must be the same identifier used when the logs were written. It can contain letters, digits, hyphens, underscores, and dots, must start with a letter or digit, and cannot contain path separators or be a reserved Windows name.
- See [.env.example](../.env.example) for the settings the CLI reads, and [manual log recovery](manual-log-recovery.txt) for what is recovered and how to operate it.
- The CLI loads the environment configuration it is given. After applying `--target`, it checks the whole application configuration: duplicated storage directories and their ownership, upload targets, the composition of failure stores, the timeout hierarchy (`must_fit` and so on), and the memory health thresholds. If anything is inconsistent, it exits with a configuration error (`invalid config: ...`, exit code 1) before starting recovery, without creating or changing any storage directory or file.
- When a value that may contain credentials (HTTP headers of `*_HEADERS`, the list of `*_S3_DESTINATIONS`) is invalid, the configuration error shows only the key and the kind of problem. The value is included neither in the message nor in errors that can be retrieved with `errors.As`.
- When `HETEROLOG_LOG_TYPE`, `HETEROLOG_ASYNC_LOG_TYPE`, and `HETEROLOG_DELIVERY_LOG_TYPE` are omitted or empty in the environment configuration, the regular, asynchronous, and delivery loggers write only to stdout. File, Slack, and the other destinations are enabled only when given explicitly, and missing credentials such as a Slack token are a configuration error.
- To recover file logs, give the same outputs as when the logs were written (for example, `HETEROLOG_LOG_TYPE=file`).
- When loading from the environment configuration, logs are also recorded in UTC unless `HETEROLOG_ENABLE_LOCAL_TIME_LOG` is set.
- The spool's `dead` and `quarantine`, and the storage for failed compressed log uploads (`retryable`, `permanent`, `dead`, `quarantine`), are not deleted automatically unless deletion is enabled in the environment configuration.
- The fileArchive for backend send failures (`retryable`, `permanent`) is not deleted automatically by default either: the limits on kept generations, age, and directory size (`HETEROLOG_MAX_*_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS`, `HETEROLOG_MAX_*_ENABLE_DURABLE_FILE_ARCHIVE_AGE`, `HETEROLOG_MAX_*_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB`) are all 0 (unlimited). Old files are deleted only by the conditions set to a non-zero value, and the allowed values are at least 2 generations, at least 1 day, and at least 10 MB and at least the maximum file size.
- `MaxFileBackups`, `MaxFileAge`, and `MaxDirSize` of `durable.FileArchiveConfig` set in code also mean unlimited when 0. To keep deleting automatically with the previous defaults (60 generations, 60 days, 2048 MB), set these values explicitly.
- So that the disk is not filled even without limits, new failures are not saved to the fileArchive while the free space is below `HETEROLOG_MIN_AVAILABLE_DISK_SPACE_GB`; they are handled by `*_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY` (`drop` discards them, `degrade` saves them to stdout). When a fileArchive is used, this threshold cannot be 0.

## Testing

```sh
go test ./...
go vet ./...
go test -race . ./internal/application/logging ./internal/application/loggerruntime ./internal/bootstrap ./internal/presentation/sloghandler ./internal/infrastructure/logger ./internal/infrastructure/logger/multi
```

- The regular tests use a dummy configuration generated from the public configuration example and local HTTP servers, and do not need a developer's `.env`.
- Tests that check the output of the file logger with the values of the configuration example need at least `HETEROLOG_MIN_AVAILABLE_DISK_SPACE_GB` (1 GB in the example) of free space on the file system of the temporary directory, and fail with a message about the missing space otherwise.
- Integration tests that connect to real services are enabled explicitly with individual environment variables.
- [testenv/windows](../testenv/windows/README.md) describes additional Windows-specific test procedures, including the CI on Windows, macOS, and Linux and the procedure to run all packages, the same as `go test ./...` in CI, on a Windows VM.
- [testenv/macos](../testenv/macos/README.md) describes how to run, on a real macOS machine, the tests with case-sensitive APFS, ExFAT, and FAT32 disk images as the temporary directory, and the integration tests on Docker.
- CI on Linux also runs the whole suite with a temporary directory reached through a symbolic link (`TMPDIR` under a symlink), as on macOS.

Case-insensitive file systems on Linux:

- The detection of case-insensitive file systems on Linux (casefold of ext4, F2FS, and tmpfs, and FAT) is checked in every run by tests that replace the file system type, the casefold attribute, and the mount options.
- The test that checks detection on real casefold directories and FAT mounts (`TestLinuxDirectoryCaseDetectsAvailableCaseInsensitiveFilesystems` of `pkg/path/pathidentity`), and the file logger tests that run only when the temporary directory is case-insensitive, are skipped in environments that cannot provide them.
- The CI job `test (ubuntu-latest, case-insensitive TMPDIR)` runs all tests with a loop-mounted ext4 casefold directory as the temporary directory, and also runs the detection on vfat mounts (and exFAT when the runner supports it) and the file logger tests in a temporary directory on vfat. This job fails if these tests are skipped.

Integration tests with real services:

- Prepare a dedicated environment file with the test destinations and credentials, and give it with `HETEROLOG_INTEGRATION_ENV_PATH` together with each test's opt-in variable. `go test` runs tests in each package's directory, so give an absolute path.
- The tests of `pkg/opensearch` also read only from this file, not from the process environment: the endpoint `HETEROLOG_OPENSEARCH_ENDPOINT`, the authentication method `HETEROLOG_OPENSEARCH_AUTH_TYPE` (`none` for no authentication; `basic` with `HETEROLOG_OPENSEARCH_USERNAME` and `HETEROLOG_OPENSEARCH_PASSWORD` for basic authentication), and the index to write to, `OPENSEARCH_TEST_INDEX` (a name starting with `app-log`; `app-log-test-index` when omitted).
- The configuration and procedure for running all integration tests against disposable containers in CI are in [testenv/integration](../testenv/integration/README.md).

```sh
OPENSEARCH_INTEGRATION_TEST=true HETEROLOG_INTEGRATION_ENV_PATH=/path/to/integration.env \
  go test ./pkg/opensearch -count=1
```

- Integration tests verify TLS certificates by default. Only for test environments with self-signed certificates, set `HETEROLOG_OPENSEARCH_SKIP_VERIFY_SSL=true` in the dedicated file. This value is `true` in `.env.example`, so set it to `false` when you create the dedicated file from `.env.example` and want certificates verified.
- The logger integration tests use the same dedicated file. Prepare a test file based on `.env.example` and set the target `HETEROLOG_OPENSEARCH_ENDPOINT`, `HETEROLOG_LOKI_ENDPOINT`, or `HETEROLOG_CLICKHOUSE_ENDPOINT` explicitly.
- The tests override the sampling rate of ERROR logs to 1, so the sampling settings of the dedicated file do not affect the results.
- For OpenSearch basic authentication, set `HETEROLOG_OPENSEARCH_USERNAME` and `HETEROLOG_OPENSEARCH_PASSWORD` in the dedicated file. Credentials are not filled in, so using OpenSearch without them is a configuration error. For an endpoint without authentication, set `HETEROLOG_OPENSEARCH_AUTH_TYPE=none`.

```sh
OPENSEARCH_INTEGRATION_TEST=true HETEROLOG_INTEGRATION_ENV_PATH=/path/to/integration.env \
  go test ./internal/infrastructure/logger -count=1
```

- Loki and ClickHouse are enabled with `LOKI_INTEGRATION_TEST=true` and `CLICKHOUSE_INTEGRATION_TEST=true`. The Tempo and OpenTelemetry Collector tests of the file logger use `TEMPO_INTEGRATION_TEST=true` and `OTEL_COLLECTOR_INTEGRATION_TEST=true` with the same dedicated file. `HETEROLOG_ENV_PATH` alone does not start tests against real services.
- While integration tests run, the settings of this library that are not in the dedicated file (the keys of `.env.example`, `HETEROLOG_ENV_PATH`, and `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, and `AWS_REGION`, which are used as defaults) are removed from the process environment. The runtime environment, such as `PATH`, `HOME`, `TMP`/`TEMP`/`TMPDIR`, `SystemRoot`, and proxy and certificate settings, is used as is.
