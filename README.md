# heterolog

[日本語版](README.ja.md)

heterolog is a logging library for Go applications whose logs must arrive. One logging call writes to several destinations at once: stdout, files, Slack, OpenSearch, Loki, CloudWatch Logs, and ClickHouse. When a log server cannot be reached, the log is kept on local disk and sent again when the server is back, even after the application restarts. You can use it through the standard `log/slog` package.

- Module path: `github.com/go-jappe/heterolog`
- Requires Go 1.27.3 or later
- License: [MIT License](LICENSE)

This README is an overview. The [reference](docs/reference.md) describes the behavior in detail, and the API is documented on [pkg.go.dev](https://pkg.go.dev/github.com/go-jappe/heterolog). Most other documents in this repository, including the comments in `.env.example`, are written in Japanese.

## Contents

- [Why heterolog](#why-heterolog)
- [Features](#features)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Sending logs to several destinations](#sending-logs-to-several-destinations)
- [Choosing a logger](#choosing-a-logger)
- [When a destination fails](#when-a-destination-fails)
- [Changing the level and sampling while running](#changing-the-level-and-sampling-while-running)
- [Log files](#log-files)
- [Traces](#traces)
- [Metrics](#metrics)
- [Shutting down](#shutting-down)
- [Recovering logs of a stopped application](#recovering-logs-of-a-stopped-application)
- [Testing](#testing)
- [Design decisions](#design-decisions)
- [License](#license)

## Why heterolog

A common setup writes logs to stdout or a file and leaves their delivery to a separate log shipper, an agent or sidecar such as Fluent Bit or Vector. heterolog sends the logs from the application itself instead:

- **The application learns what happened to each log.** The regular API returns an error when a log could not be written or kept, and the delivery API reports the result for each destination. With a shipper, the application only knows that it wrote to a local file.
- **Logs that could not be sent are not lost.** They are saved to a local spool and sent again when the destination recovers, following `Retry-After`, also after a restart. This covers what the disk buffer of an agent usually provides. A log whose outcome is unknown is set aside for a person to check, instead of being sent twice or deleted.
- **There is nothing else to deploy.** There is no extra process to size, monitor, and upgrade next to each application, and no intermediate file or socket that can fill up or stall. This also suits Go programs that are themselves agents, daemons, or command-line tools.
- **The level can change without a restart.** For example, switch an output to DEBUG with one HTTP request during an investigation, and back again afterwards.

The trade-off is that delivery uses the application's own CPU, memory, and network, and changing destinations means restarting the application with new settings. Another tool may fit better:

- If you only need structured logs on stdout, `log/slog` alone, or a library such as zap or zerolog, is simpler.
- If many programs written in different languages share a host, or routing must be changed in one central place, a dedicated log shipper is usually the better choice.

## Features

- Destinations: stdout, file, Slack, OpenSearch, Loki, CloudWatch Logs, and ClickHouse, in any combination.
- Three loggers with different guarantees: synchronous (returns errors), asynchronous (best effort), and delivery (reports the result for each destination).
- A `log/slog` handler.
- Log files with rotation, compression (gzip, zstd, lz4, xz), retention, and upload to S3 or S3-compatible storage such as Backblaze B2.
- A local spool for logs that could not be sent, replayed automatically and following `Retry-After` across restarts.
- Overload protection with a free disk space threshold, an optional memory pressure monitor, and `drop`/`degrade` policies.
- Configuration in code, or from `HETEROLOG_` environment variables and an env file.
- Level and sampling changes while running, in code or through a token-protected HTTP API, which also reports the configuration in use and the status of the process, destinations, spools and disks, with warnings, and serves a browser page for all of it.
- Traces: a `Tracer`, and an exporter for the OpenTelemetry SDK, that send spans to an OTLP/HTTP receiver, OpenSearch (for its Trace Analytics), ClickHouse (in the table of the OpenTelemetry Collector), a file, or stdout in batches with retries. Spans that could not be sent go to the same spool and failure archive as logs. The loggers can record their own work as spans.
- Metrics: a `Meter` that sends counters, gauges, and histograms to an OTLP/HTTP receiver, VictoriaMetrics, a file, or stdout, with the same retries, spool, and failure archive.
- A CLI that recovers the log files of a stopped application.
- Tested on Linux, Windows, and macOS, including case-insensitive file systems.

## Installation

```sh
go get github.com/go-jappe/heterolog
```

## Quick start

The following program writes two logs to stdout. A similar program is in [examples/slog](examples/slog/main.go); in a checkout of this repository, run it with `go run ./examples/slog`.

```go
package main

import (
    "context"
    "log"
    "log/slog"

    "github.com/go-jappe/heterolog"
)

func main() {
    ctx := context.Background()
    runtime, err := heterolog.New(ctx, heterolog.DefaultConfig())
    if err != nil {
        log.Fatal(err)
    }
    defer func() {
        if err := runtime.Close(); err != nil {
            log.Print(err)
        }
    }()

    logger := slog.New(heterolog.NewSlogHandler(runtime, nil))
    logger.InfoContext(ctx, "service started", "service", "example")
    logger.With("component", "worker").WithGroup("job").InfoContext(ctx,
        "job completed", "id", 42)
}
```

By default, INFO logs are written to standard output and WARN and ERROR logs to standard error, with timestamps in UTC. To use it as the global logger, call `slog.SetDefault(logger)`. `slog` and each output filter by level separately, so to write DEBUG logs, lower both levels (see [Using slog](docs/reference.md#using-slog)).

## Sending logs to several destinations

The quickest way is an env file that `LoadEnvConfig` reads. This one writes to stdout, a log file, and Loki:

```sh
# logging.env
HETEROLOG_TARGET=my-service
HETEROLOG_LOG_TYPE=stdout,file,loki
HETEROLOG_LOKI_ENDPOINT=http://loki.example.internal:3100
```

```go
env, err := heterolog.LoadEnvConfig("logging.env") // "" reads only environment variables
if err != nil {
    log.Fatal(err)
}
runtime, err := env.New(ctx) // env.NewAsync and env.NewDelivery work the same way
if err != nil {
    log.Fatal(err)
}
defer runtime.Close()

logger := slog.New(heterolog.NewSlogHandler(runtime, nil))
logger.InfoContext(ctx, "service started")
```

- `HETEROLOG_TARGET` is the name of the application in file and directory names. By default, log files go under `$XDG_STATE_HOME` (or `~/.local/state`) on Linux and the BSDs (`/var/log` when running as root), `~/Library/Logs` on macOS, and `%LOCALAPPDATA%` on Windows. This example writes `my-service/logs/my-service_<date>_1.log` there, and ERROR and DEBUG logs to files of their own.
- Settings that you leave out use their defaults, so the file only needs what differs. [.env.example](.env.example) lists every key (its comments are in Japanese). Its values are a sample configuration and differ from the defaults in places.
- Without any settings, every logger writes to stdout only. Other destinations are used only when you select them, and missing credentials are reported as errors instead of being filled in.
- The settings are checked when they are loaded, and all problems are reported together.
- A variable set in the environment overrides the same key in the file. The process environment is not modified, and no other file is read.

### Default levels and sampling

Unless you set them, every output records INFO and above, except Slack, which is meant for notifications and records ERROR and above. Sampling is off, so every log at or above the level is kept:

| Output | Level | Sampling |
| --- | --- | --- |
| Slack | ERROR | off |
| stdout, file, OpenSearch, Loki, CloudWatch Logs, ClickHouse | INFO | off |

- The keys are `HETEROLOG_<OUTPUT>_LOG_LEVEL` for the level and `HETEROLOG_ENABLE_LOG_<OUTPUT>_SAMPLING` for sampling, where `<OUTPUT>` is `STDOUT`, `FILE`, `SLACK`, `OPENSEARCH`, `LOKI`, `CLOUDWATCH`, or `CLICKHOUSE`. The asynchronous logger has its own keys with `ASYNC_` in the name.
- With sampling on, a rate of N keeps 1 in N logs of that level, and logs that carry a request ID (`heterolog.WithRequestID`, or a string `request_id` attribute in slog) can be kept or skipped by request instead, so the logs of a kept request are complete. Set the rates when you turn sampling on: rates you leave out keep their defaults, such as 1 in 1,000 INFO logs for OpenSearch, Loki, CloudWatch Logs, and ClickHouse.
- The level and sampling can also be [changed while running](#changing-the-level-and-sampling-while-running).
- The delivery logger does not sample, and only its Slack output filters by level (ERROR).

### Configuring in code

`heterolog.DefaultConfig()` returns a configuration that writes to stdout. Set other fields of `Config`, such as `File` and `Loki`, to add destinations; the [pkg/logger documentation](https://pkg.go.dev/github.com/go-jappe/heterolog/pkg/logger) describes every setting. To start from the environment and adjust the configuration in code, get it with `cfg, opts, err := env.Config()` and pass it to `heterolog.New(ctx, cfg, opts...)`.

## Choosing a logger

There are three loggers. They differ in what a logging call tells the caller:

| Constructor | A logging call returns | Use it for |
| --- | --- | --- |
| `New` | an `error` | logs that must not be lost silently |
| `NewAsync` | nothing, without waiting for the send | high-volume logs |
| `NewDelivery` | a `DeliveryReport` and an `error` | logs whose next step depends on where they arrived |

`NewSlogHandler` turns a logger from `New` into a `slog.Handler`. `slog.Logger` drops the errors of its handler, so call the logger directly when you need them:

```go
jobID := heterolog.MustFieldKey("job_id")
if err := runtime.Info(ctx, "job completed", jobID.Int(42)); err != nil {
    // The log could not be written or kept.
}
```

A delivery report has the result of each destination, and tells confirmed success apart from failure, rejection before sending, an unknown outcome, and acceptance into a send queue:

```go
delivery, err := heterolog.NewDelivery(ctx, heterolog.DefaultDeliveryConfig())
if err != nil {
    log.Fatal(err)
}
defer delivery.Close()

report, err := delivery.Info(ctx, "payment captured")
if err != nil || report.HasUnregisteredTarget() {
    // Some destination has not confirmed the log; report.Targets has the result of each.
}
```

A successful call of the regular API can mean that the log was accepted into a buffer; it does not guarantee that the log arrived. Only the delivery logger reports arrival.

## When a destination fails

For OpenSearch, Loki, CloudWatch Logs, and ClickHouse, a log that could not be sent is handled according to the failure:

| What happened | What heterolog does |
| --- | --- |
| A temporary failure, such as a timeout, 429, or 5xx | Saves the log to the spool and sends it again later, following `Retry-After`, also after a restart |
| The outcome is unknown, for example because the connection broke after sending | Moves the log to `quarantine`. It is not sent again automatically, because it may already have arrived |
| The destination rejected the content of the log | Splits the batch to find the rejected logs, and moves only those to `quarantine` |
| The destination or the settings are wrong, such as rejected authentication or a missing table | Moves the logs to `dead` |

- Saved logs are not deleted by default, because they may be the only copy. To keep the disk from filling up, new failures are instead discarded (`drop`, reported as `ErrLogDropped`) or written to stdout (`degrade`) while free space is below `HETEROLOG_MIN_AVAILABLE_DISK_SPACE_GB`.
- When the choice is between sending a log twice and losing it, heterolog sends it twice.
- With the environment configuration, the regular and asynchronous loggers use the spool by default. The delivery logger does not replay: it saves failed logs to local files and reports every result to the caller.
- Slack keeps no failed logs. Log files that could not be uploaded to S3 are kept and uploaded again.

The [reference](docs/reference.md#failure-handling-and-replay) describes the rules in detail.

## Changing the level and sampling while running

Each output of the regular and asynchronous loggers has a level and sampling settings that you can change while the application runs, without restarting it or rebuilding the logger. A change applies from the next log and is kept in memory only, so a restart returns to the configured values, and a temporary change cannot stay by accident.

To change them from outside the process, start the admin API:

```sh
# logging.env
# An empty host listens on 127.0.0.1 only.
HETEROLOG_ADMIN_ADDR=:9100
# At least 32 bytes, for example the output of: openssl rand -hex 32
HETEROLOG_ADMIN_TOKEN=<token>
```

```go
if adminCfg, ok := env.AdminServerConfig(); ok {
    // Records every change request; by default to stdout and a file in audit_logs.
    audit, err := env.NewAdminAudit(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer audit.Close()
    members := heterolog.AdminMembers{
        // Loggers: the settings can be read and changed, and the configuration is reported.
        Loggers: map[string]heterolog.SettingsController{"app": runtime},
        // DeliveryLoggers and Telemetries: the configuration is reported.
        DeliveryLoggers: map[string]heterolog.DeliveryLogger{"audit": audit},
    }
    admin, err := heterolog.StartAdminServer(adminCfg, members, audit)
    if err != nil {
        log.Fatal(err)
    }
    defer admin.Close()
}
```

The name given to each member (here `app` and `audit`) is its key in the JSON:

```sh
# Show the settings of every logger
curl -H "Authorization: Bearer $HETEROLOG_ADMIN_TOKEN" http://127.0.0.1:9100/settings

# Show the configuration every member was created with (tokens and keys are masked)
curl -H "Authorization: Bearer $HETEROLOG_ADMIN_TOKEN" http://127.0.0.1:9100/config

# Show one value of it, or several in one request
curl -H "Authorization: Bearer $HETEROLOG_ADMIN_TOKEN" http://127.0.0.1:9100/config/loggers/app/config/timeout
curl -H "Authorization: Bearer $HETEROLOG_ADMIN_TOKEN" \
  "http://127.0.0.1:9100/config?path=loggers/app/config/timeout&path=loggers/app/config/max_log_entry_bytes"

# Show the status: CPU and memory, every destination, spools, directories, and warnings
curl -H "Authorization: Bearer $HETEROLOG_ADMIN_TOKEN" http://127.0.0.1:9100/status
curl -H "Authorization: Bearer $HETEROLOG_ADMIN_TOKEN" http://127.0.0.1:9100/status/warnings

# Write DEBUG logs to stdout
curl -X PATCH -H "Authorization: Bearer $HETEROLOG_ADMIN_TOKEN" \
  -d '{"loggers":{"app":{"outputs":{"stdout":{"level":"DEBUG"}}}}}' \
  http://127.0.0.1:9100/settings

# Keep 1 in 10 INFO logs in the log file
curl -X PATCH -H "Authorization: Bearer $HETEROLOG_ADMIN_TOKEN" \
  -d '{"loggers":{"app":{"outputs":{"file":{"sampling":{"enabled":true,"by_request_id":false,"debug_rate":1,"info_rate":10,"warn_rate":1,"error_rate":1}}}}}}' \
  http://127.0.0.1:9100/settings
```

Every request returns the settings of every logger. For example, this is the stdout part after the second request (the file and loki outputs are omitted):

```json
{
  "loggers": {
    "app": {
      "outputs": {
        "stdout": {
          "level": "DEBUG",
          "sampling": {
            "enabled": false,
            "by_request_id": true,
            "request_id_rate": 10,
            "debug_rate": 1,
            "info_rate": 1,
            "warn_rate": 1,
            "error_rate": 1
          }
        }
      }
    }
  }
}
```

- A rate of N keeps 1 in N logs of that level. With `by_request_id`, logs that carry a request ID are kept or skipped by request (1 in `request_id_rate` requests), so the logs of a kept request are complete.
- Levels are `DEBUG`, `INFO`, `WARN`, and `ERROR`. Fields omitted in a `PATCH` keep their values. A `PATCH` is checked as a whole: if any change is invalid, nothing is applied, and the response is `400` with the reason.
- Every request needs the token. It is compared in constant time and never appears in errors. To listen on an address other than 127.0.0.1, give the host and a TLS certificate and key (`HETEROLOG_ADMIN_TLS_CERT_FILE` and `HETEROLOG_ADMIN_TLS_KEY_FILE`), so the token is not sent in clear text.
- Changes are processed one at a time, and each is recorded in an audit log: the request, the settings when the change starts, the settings after it, and every rejected request (only a successful `GET` is not recorded). Every record has the field `log_kind=audit` and the request ID, which the response also returns in `X-Request-Id`.
- The audit log is written by a delivery logger of its own, by default to stdout and to a file in `audit_logs`, next to the other log directories. Other outputs, such as CloudWatch Logs, can be chosen with `HETEROLOG_ADMIN_AUDIT_LOG_TYPE`; see [Audit log of the admin API](docs/reference.md#audit-log-of-the-admin-api).
- If the settings after a change do not match the request, the change is not reverted: the response is `500` with the current settings, and the mismatch is recorded at ERROR, which a Slack output of the audit log turns into a notification. If an audit record does not reach every output, the change still goes ahead and the response carries a warning.
- To serve the API from your own HTTP server, use `NewAdminHandler` with the same audit logger. In code, `runtime.OutputSettings()` returns the settings and `runtime.UpdateOutputSettings(changes)` changes them.
- Only the level and sampling can change while running; destinations and storage cannot. The delivery logger has no such settings.
- `GET /config` returns the configuration each member was created with, as JSON with `snake_case` keys, and `GET /config/<path>` returns one value of it (`GET /config?path=...&path=...` several). Tokens, passwords, and keys are shown as `***`. Delivery loggers and telemetries added to `AdminMembers` are reported too.
- The same server serves a browser page at `/`: open `http://127.0.0.1:9100/`, enter the token (kept in the browser's session storage, so it is gone when the tab closes), and view the status with its warnings, change the level and sampling, and read the configuration. The page is English with a Japanese switch and loads nothing from other sites.
- `GET /status` returns the CPU and memory of the process, the records, request durations and retries of every destination, the state of each spool (files per state, resend, last purge) and the directories in use with their free disk space, together with `warnings` (a disk nearly full, a slow or failing destination, a stopped resend, and so on), critical ones first. File counts come from a background scan every 5 minutes (`HETEROLOG_ADMIN_STATUS_DIRECTORY_SCAN_INTERVAL_SECOND`, 0 turns it off) and are approximate. See [Status of the loggers](docs/reference.md#status-of-the-loggers).

See [Changing settings while running](docs/reference.md#changing-settings-while-running) for details.

## Log files

- Rotation every day, week (from Sunday 0:00 UTC), or month, and to a new file when a file reaches its maximum size.
- ERROR and DEBUG logs can go to files of their own (the environment configuration does so by default).
- Compression with gzip, zstd, lz4, or xz. A compressed file is compared with the original log before the original is deleted.
- Retention by the number of generations, age, and directory size.
- Upload of compressed files to S3, with upload records that survive restarts.
- File names are matched according to the case rule of the file system (Windows, macOS, and case-insensitive directories on Linux).

The [reference](docs/reference.md#log-files) describes rotation, retention, compression, and uploads in detail.

## Traces

`NewTelemetry` sends spans to an OTLP/HTTP receiver, such as the OpenTelemetry Collector or Grafana Tempo, to OpenSearch, to ClickHouse, to a file, or to stdout. Its `Tracer` records the spans of your application, and passed to a logger with `WithObservers`, also the work of the logger as child spans:

```go
cfg := heterolog.DefaultTelemetryConfig()
cfg.Resource.ServiceName = "checkout"
cfg.Traces.Exporters = []heterolog.TraceExporter{heterolog.TraceExporterOTLP}
cfg.Traces.OTLP.Endpoint = "http://localhost:4318/v1/traces"
tel, err := heterolog.NewTelemetry(ctx, cfg)
if err != nil {
    log.Fatal(err)
}
defer tel.Shutdown(context.Background())

runtime, err := heterolog.New(ctx, heterolog.DefaultConfig(),
    heterolog.WithObservers(tel.Tracer(), nil))
if err != nil {
    log.Fatal(err)
}
defer runtime.Close()

ctx, span := tel.Tracer().StartSpan(ctx, "checkout")
defer span.End()
```

- From the environment, set `HETEROLOG_ENABLE_TRACES=true` and `HETEROLOG_TRACES_OTLP_ENDPOINT`, and call `env.NewTelemetry(ctx)`. The service name comes from `HETEROLOG_APP_NAME`.
- Spans are sent in batches from a queue for each destination, so ending a span never waits for the network. Failed requests are retried. Spans that the receiver still did not accept can be kept in the same spool and failure archive as logs, and the spool resends them when the receiver recovers, also after a restart. Spans that are not kept, or that do not fit in the queue, are dropped, counted in metrics, and reported to the function passed with `WithTelemetryErrorHandler`, or to the standard logger without it.
- `TraceExporterOpenSearch` writes each span as a document in the format of OpenSearch Data Prepper, so the Trace Analytics of OpenSearch Dashboards can show the spans. Install the [index template](docs/opensearch/otel-v1-apm-span-index-template.json) first.
- `TraceExporterClickHouse` inserts each span into a table with the columns of the `otel_traces` table of the OpenTelemetry Collector, which the ClickHouse data source of Grafana reads. Create it with [otel_traces.sql](docs/clickhouse/otel_traces.sql) first.
- `pkg/observer` reads and writes the W3C `traceparent` header, so a trace can continue across services.
- Spans from the OpenTelemetry SDK are sent the same way through `otelexport.NewSpanExporter(tel)`, a separate package so that programs without the SDK do not link it.

See [Sending traces](docs/reference.md#sending-traces) for details.

## Metrics

The same `Telemetry` sends metrics to an OTLP/HTTP receiver, such as the OpenTelemetry Collector, Prometheus, or VictoriaMetrics, to the Prometheus import API of VictoriaMetrics, to a file, or to stdout. Its `Meter` records counters, gauges, and histograms, and passed to a logger with `WithObservers`, also the `heterolog_*` metrics of the logger:

```go
cfg.Metrics.Exporters = []heterolog.MetricExporter{heterolog.MetricExporterOTLP}
cfg.Metrics.OTLP.Endpoint = "http://localhost:4318/v1/metrics"
tel, err := heterolog.NewTelemetry(ctx, cfg)

tel.Meter().Counter("orders_total").Add(ctx, 1, heterolog.ObserverAttr{Key: "region", Value: "eu"})
```

- From the environment, set `HETEROLOG_ENABLE_METRICS=true` and `HETEROLOG_METRICS_OTLP_ENDPOINT`.
- The values are collected every `ExportInterval` (one minute by default) and sent as cumulative values, so a collection that is dropped or lost still leaves the totals in the next one.
- Metrics from the OpenTelemetry SDK are sent the same way through `otelexport.NewMetricExporter(tel)`, passed to an SDK reader.
- Metrics that could not be sent go to the same spool and failure archive as logs and spans. Because an uncertain send is resent rather than lost, the same value can arrive twice; with cumulative values, the second one is the same value at the same time.

See [Sending metrics](docs/reference.md#sending-metrics) for details.

## Shutting down

- Call `Close` or `Shutdown(ctx)` before the application exits. They finish accepted work, flush buffers, and release resources. A `Telemetry` sends its queued spans, and collects and sends its metrics one last time, in `Shutdown(ctx)`; close the loggers that use its `Tracer` and `Meter` first.
- When the context of `Shutdown` ends, only that call stops waiting; the cleanup continues. `ForceClose` asks the cleanup to stop and returns without waiting.
- Logs left in the spool are sent after the next start.

See [Shutdown](docs/reference.md#shutdown) for details.

## Recovering logs of a stopped application

The repository also has a CLI that recovers the log files of a stopped application, with the same environment configuration as the application:

```sh
go build -o log-recovery ./cmd/log-recovery
./log-recovery --target my-service --application-stopped --env-file /path/to/logging.env
```

See [Log recovery CLI](docs/reference.md#log-recovery-cli) for its checks and options, and [manual log recovery](docs/manual-log-recovery.txt) (in Japanese) for what it recovers and how to operate it.

## Testing

```sh
go test ./...
go vet ./...
```

The regular tests need no external service. CI also runs the whole suite on Windows and macOS, and on Linux with a case-insensitive and with a symlinked temporary directory, and runs integration tests against OpenSearch, Loki, ClickHouse, Tempo, and the OpenTelemetry Collector in containers. See [Testing](docs/reference.md#testing) for race tests and integration tests with real services.

## Design decisions

### Three loggers, and keeping logs that failed

Logs differ in how much the caller needs to know about them, so heterolog has three loggers instead of one logger with options:

- `New` is synchronous and returns an error when a log could not be written or kept. It is for logs that must not be lost silently.
- `NewAsync` is best effort: it does not make the caller wait for delivery and reports no per-call errors. It is for high-volume logs.
- `NewDelivery` returns a report for each destination that separates confirmed success, failure, rejection before sending, an unknown outcome, and queuing. It is for callers that decide what to do next based on where a log arrived.
- Each type states its guarantee in its method signatures (a logging call of the synchronous logger returns an error, one of the asynchronous logger returns nothing, and one of the delivery logger returns a report), so the code shows which guarantee it relies on.

A log that cannot be delivered is kept, not discarded:

- Temporary failures go to the spool and are replayed. `Retry-After` is honored and survives restarts, so a recovering destination is not flooded.
- When the outcome is unknown, for example because the connection broke after sending, the log goes to `quarantine` instead of being resent automatically: resending could deliver it twice, and deleting it could lose it, so a person decides.
- Logs rejected because of their content are isolated one by one, by splitting batches, so that one bad entry does not block the others. Failures caused by the destination or the configuration, such as rejected authentication or a missing table, go to `dead`.
- When the choice is between a duplicate and a loss, heterolog prefers the duplicate.

Kept failures are not deleted by default. They may be the only copy of a log, so deleting them should be an explicit decision, and retention limits are opt-in. Filling the disk is prevented instead by the free space threshold (`HETEROLOG_MIN_AVAILABLE_DISK_SPACE_GB`): below it, new failures are dropped (reported as `ErrLogDropped`) or degraded to stdout, as configured.

### Configuration

- Configuration is explicit. Importing the package or constructing a logger has no side effects: it does not load env files, install signal handlers, or replace the global slog logger or OpenTelemetry providers. A library should not change the state of the application that uses it.
- Reading the environment is opt-in. `LoadEnvConfig` reads only when it is called and only the file it is given, and never modifies the process environment. A variable set in the environment overrides the file, as most dotenv tools do.
- Every key starts with `HETEROLOG_`, so the settings can live in the same environment and `.env` file as the application's own settings without colliding.
- With no settings, every logger writes only to stdout, so heterolog works without any configuration or external service. Other destinations are enabled only when they are selected, and missing credentials are reported as errors instead of being filled in.
- Sampling is opt-in. By default, every output records every log at INFO and above (Slack, which is meant for notifications, at ERROR and above), so no log at or above the level is skipped unless you turn sampling on.
- Settings are checked when they are loaded or when a logger is constructed, and all problems are reported together, so a wrong setting fails at startup instead of at the first log or the first failure.
- Only the level and sampling can change while running. The outputs read them for every entry, so a change needs neither rebuilding loggers nor interrupting logging; changing destinations or storage would. The HTTP API needs a token, listens only on 127.0.0.1 unless TLS is configured, and its changes do not survive a restart, so a temporary change, such as switching to DEBUG during an investigation, cannot stay by accident.
- Configuration errors do not include values that may contain credentials, such as HTTP headers, lists of S3 destinations, and the user info of URLs.
- Defaults follow the platform: timestamps are in UTC, and on Linux, the BSDs, illumos, Solaris and AIX, storage defaults to the user's state directory (`$XDG_STATE_HOME` if it is an absolute path, or else `~/.local/state`) when the process does not run as root, so a regular user can enable file output without extra settings.

### Testing on real systems

- The parts most likely to lose or duplicate logs depend on the operating system and the file system: case sensitivity of names, renaming without replacement, syncing directories, file permissions and Windows ACLs, symbolic links in temporary directories, and macOS AppleDouble files. Replaced inputs alone do not reproduce these behaviors, so besides unit tests, the suite runs on real file systems.
- CI runs the whole suite on Linux, Windows, and macOS, plus Linux jobs with a temporary directory reached through a symbolic link and with a case-insensitive temporary directory (ext4 casefold and vfat), and integration tests against OpenSearch, Loki, ClickHouse, Tempo, and the OpenTelemetry Collector in containers.
- In the jobs that exist to cover such a condition, a test that is skipped counts as a failure, so the coverage cannot disappear silently.
- Beyond CI, the whole suite has been run on a Windows virtual machine, as a regular user and as an administrator, and on a real macOS machine with case-sensitive APFS, ExFAT, and FAT32 disk images. The results are recorded in [testenv](testenv).
- Timing-dependent behavior is tested with virtual time (`testing/synctest`) where possible.

## License

[MIT License](LICENSE)
