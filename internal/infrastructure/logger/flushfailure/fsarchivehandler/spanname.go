package fsarchivehandler

const (
	SpanFileCompress                 = "infra.logger.flushfailure.filearchive.compress"
	SpanFileArchiveUpload            = "infra.logger.flushfailure.filearchive.upload"
	SpanFailureArchivePipelineCreate = "infra.logger.flushfailure.filearchive.create"
	SpanFileWrite                    = "infra.logger.flushfailure.filearchive.write"
	SpanRetryableArchiveWrite        = "infra.logger.flushfailure.filearchive.retryable.write"
	SpanPermanentArchiveWrite        = "infra.logger.flushfailure.filearchive.permanent.write"
	SpanDelegate                     = "infra.logger.flushfailure.filearchive.delegate"
	SpanFilePurgeByAge               = "infra.logger.failurearchive.purge.age"
	SpanFilePurgeByGeneration        = "infra.logger.failurearchive.purge.generation"
	SpanFilePurgeByDirectorySize     = "infra.logger.failurearchive.purge.dirsize"
)
