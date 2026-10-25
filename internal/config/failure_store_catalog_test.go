package config

import (
	"errors"
	"testing"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestValidateFailureStoreCatalogAcceptsCurrentCatalog(t *testing.T) {
	if err := validateFailureStoreCatalog(AppConfig{}); err != nil {
		t.Fatalf("validateFailureStoreCatalog() error = %v, want nil", err)
	}
}

func TestValidateFailureStoreCatalogRejectsDestinationMissingFromCatalog(t *testing.T) {
	cfg := AppConfig{}
	destinations := append(
		pkglogger.AllLogOutputDestinations(),
		pkglogger.LogOutputDestination("future-backend"),
	)
	err := validateFailureStoreCatalogEntries(
		cfg,
		destinations,
		cfg.failureStoreCatalog(),
	)
	if !errors.Is(err, errFailureStoreCatalogMissingDestination) {
		t.Fatalf("missing destination error = %v, want %v", err, errFailureStoreCatalogMissingDestination)
	}
}

func TestValidateFailureStoreCatalogRejectsUnclassifiedCatalogEntry(t *testing.T) {
	cfg := AppConfig{}
	entries := cfg.failureStoreCatalog()
	entries[0].support = failureStoreSupportUnclassified

	err := validateFailureStoreCatalogEntries(
		cfg,
		pkglogger.AllLogOutputDestinations(),
		entries,
	)
	if !errors.Is(err, errFailureStoreCatalogUnclassified) {
		t.Fatalf("unclassified destination error = %v, want %v", err, errFailureStoreCatalogUnclassified)
	}
}

func TestValidateFailureStoreCatalogRejectsWiringOnUnsupportedDestination(t *testing.T) {
	cfg := AppConfig{}
	entries := cfg.failureStoreCatalog()
	entries[0].syncStore.source = "logger.file"

	err := validateFailureStoreCatalogEntries(
		cfg,
		pkglogger.AllLogOutputDestinations(),
		entries,
	)
	if !errors.Is(err, errFailureStoreCatalogUnsupportedWiring) {
		t.Fatalf("unsupported destination wiring error = %v, want %v", err, errFailureStoreCatalogUnsupportedWiring)
	}
}

func TestFailureStoreCatalogClassifiesEveryLogOutputDestination(t *testing.T) {
	allDestinations := pkglogger.AllLogOutputDestinations()
	cfg := AppConfig{}
	cfg.Logger.LogOutputDestinations = allDestinations
	cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations = allDestinations
	cfg.DeliveryLogger.LogOutputDestinations = allDestinations

	entries := cfg.failureStoreCatalog()
	registered := make(map[pkglogger.LogOutputDestination]failureStoreCatalogEntry, len(entries))
	for _, entry := range entries {
		if !entry.destination.IsValid() {
			t.Fatalf("failure store catalog contains invalid destination %q", entry.destination)
		}
		if _, duplicated := registered[entry.destination]; duplicated {
			t.Fatalf("failure store catalog contains destination %q more than once", entry.destination)
		}
		registered[entry.destination] = entry

		switch entry.support {
		case failureStoreSupportSupported:
			wantBackend := entry.destination.String()
			if got, want := entry.syncStore.source, "logger."+wantBackend; got != want {
				t.Fatalf("%s sync source = %q, want %q", entry.destination, got, want)
			}
			if got, want := entry.asyncStore.source, "async_logger."+wantBackend; got != want {
				t.Fatalf("%s async source = %q, want %q", entry.destination, got, want)
			}
			if got, want := entry.deliveryStore.source, "delivery_logger."+wantBackend; got != want {
				t.Fatalf("%s delivery source = %q, want %q", entry.destination, got, want)
			}
			if !entry.syncStore.selected || !entry.asyncStore.selected || !entry.deliveryStore.selected {
				t.Fatalf(
					"%s selection = sync:%t async:%t delivery:%t, want all selected",
					entry.destination,
					entry.syncStore.selected,
					entry.asyncStore.selected,
					entry.deliveryStore.selected,
				)
			}
		case failureStoreSupportUnsupported:
			if err := validateUnsupportedFailureStoreEntry(entry); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf(
				"destination %q has failure store support %d, want an explicit classification",
				entry.destination,
				entry.support,
			)
		}
	}

	for _, destination := range allDestinations {
		if _, exists := registered[destination]; !exists {
			t.Fatalf("destination %q is missing from the failure store catalog", destination)
		}
	}
}

func TestFailureStoreCatalogMapsSelectionIndependentlyForEachLoggerMode(t *testing.T) {
	cfg := AppConfig{}
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{
		pkglogger.LogOutputOpenSearch,
	}
	cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations = pkglogger.LogOutputDestinations{
		pkglogger.LogOutputLoki,
	}
	cfg.DeliveryLogger.LogOutputDestinations = pkglogger.LogOutputDestinations{
		pkglogger.LogOutputClickHouse,
	}

	want := map[pkglogger.LogOutputDestination][3]bool{
		pkglogger.LogOutputOpenSearch:     {true, false, false},
		pkglogger.LogOutputLoki:           {false, true, false},
		pkglogger.LogOutputCloudWatchLogs: {false, false, false},
		pkglogger.LogOutputClickHouse:     {false, false, true},
	}

	for _, entry := range cfg.failureStoreCatalog() {
		if entry.support != failureStoreSupportSupported {
			continue
		}
		got := [3]bool{
			entry.syncStore.selected,
			entry.asyncStore.selected,
			entry.deliveryStore.selected,
		}
		if got != want[entry.destination] {
			t.Fatalf(
				"%s selection = sync:%t async:%t delivery:%t, want sync:%t async:%t delivery:%t",
				entry.destination,
				got[0],
				got[1],
				got[2],
				want[entry.destination][0],
				want[entry.destination][1],
				want[entry.destination][2],
			)
		}
	}
}

func TestFailureStoreCatalogMapsEverySupportedLoggerModeToItsBackend(t *testing.T) {
	cfg := AppConfig{}

	cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig.MaxConcurrent = 101
	cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.
		OpenSearchFlushManagerConfig.FailureHandlerConfig.MaxConcurrent = 102
	cfg.DeliveryLogger.DeliveryOpenSearchConfig.DeliveryOpenSearchFlushManagerConfig.
		FailureHandlerConfig.FileArchiveConfig.Dir = "delivery-opensearch"

	cfg.Logger.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig.MaxConcurrent = 201
	cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.
		LokiFlushManagerConfig.FailureHandlerConfig.MaxConcurrent = 202
	cfg.DeliveryLogger.DeliveryLokiConfig.DeliveryLokiFlushManagerConfig.
		FailureHandlerConfig.FileArchiveConfig.Dir = "delivery-loki"

	cfg.Logger.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.
		FailureHandlerConfig.MaxConcurrent = 301
	cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.
		CloudWatchLogsFlushManagerConfig.FailureHandlerConfig.MaxConcurrent = 302
	cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.DeliveryCloudWatchLogsFlushManagerConfig.
		FailureHandlerConfig.FileArchiveConfig.Dir = "delivery-cloudwatchlogs"

	cfg.Logger.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig.MaxConcurrent = 401
	cfg.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.
		ClickHouseFlushManagerConfig.FailureHandlerConfig.MaxConcurrent = 402
	cfg.DeliveryLogger.DeliveryClickHouseConfig.DeliveryClickHouseFlushManagerConfig.
		FailureHandlerConfig.FileArchiveConfig.Dir = "delivery-clickhouse"

	expected := map[pkglogger.LogOutputDestination]struct {
		syncMarker     int
		asyncMarker    int
		deliveryMarker string
	}{
		pkglogger.LogOutputOpenSearch: {
			syncMarker:     101,
			asyncMarker:    102,
			deliveryMarker: "delivery-opensearch",
		},
		pkglogger.LogOutputLoki: {
			syncMarker:     201,
			asyncMarker:    202,
			deliveryMarker: "delivery-loki",
		},
		pkglogger.LogOutputCloudWatchLogs: {
			syncMarker:     301,
			asyncMarker:    302,
			deliveryMarker: "delivery-cloudwatchlogs",
		},
		pkglogger.LogOutputClickHouse: {
			syncMarker:     401,
			asyncMarker:    402,
			deliveryMarker: "delivery-clickhouse",
		},
	}

	seen := make(map[pkglogger.LogOutputDestination]struct{}, len(expected))
	for _, entry := range cfg.failureStoreCatalog() {
		if entry.support != failureStoreSupportSupported {
			continue
		}
		want, exists := expected[entry.destination]
		if !exists {
			t.Fatalf("unexpected failure store destination %q", entry.destination)
		}
		seen[entry.destination] = struct{}{}
		if got := entry.syncStore.config.MaxConcurrent; got != want.syncMarker {
			t.Fatalf("%s sync config marker = %d, want %d", entry.destination, got, want.syncMarker)
		}
		if got := entry.asyncStore.config.MaxConcurrent; got != want.asyncMarker {
			t.Fatalf("%s async config marker = %d, want %d", entry.destination, got, want.asyncMarker)
		}
		if got := entry.deliveryStore.config.FileArchiveConfig.Dir; got != want.deliveryMarker {
			t.Fatalf(
				"%s delivery config marker = %q, want %q",
				entry.destination,
				got,
				want.deliveryMarker,
			)
		}
	}
	if len(seen) != len(expected) {
		t.Fatalf("mapped supported destinations = %d, want %d", len(seen), len(expected))
	}
}
