package worker

import "errors"

var (
	ErrMissingProcessingRedistributionPlan = errors.New(
		"missing processing redistribution plan",
	)
	ErrWorkerActiveSpoolWriterNil = errors.New(
		"worker active spool writer is nil",
	)
	// ErrWorkerStoreNilは、構築時にwork file storeがnilであることを表します。
	ErrWorkerStoreNil = errors.New(
		"worker store is nil",
	)
	// ErrWorkerSenderNilは、構築時にsenderがnilであることを表します。
	ErrWorkerSenderNil = errors.New(
		"worker sender is nil",
	)
	// ErrNilProcessingFileTransitionRetryConfigは、状態遷移retryの設定providerがnilであることを表します。
	ErrNilProcessingFileTransitionRetryConfig = errors.New(
		"processing file transition retry config is nil",
	)
	ErrInvalidProcessingFileTransitionRetryDelay = errors.New(
		"processing file transition retry delay must be positive",
	)
	ErrInvalidProcessingFileTransitionRetryMaxEntries = errors.New(
		"processing file transition retry max entries must be positive",
	)
	ErrInvalidMaxRetryAfter = errors.New(
		"spool max retry-after must not be negative",
	)
)
