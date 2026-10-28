package fsarchivehandler

const (
	SpanFileCompress                 = "heterolog.failurearchive.compress"
	SpanFailureArchivePipelineCreate = "heterolog.failurearchive.create"
	SpanFileWrite                    = "heterolog.failurearchive.write"
	SpanRetryableArchiveWrite        = "heterolog.failurearchive.write.retryable"
	SpanPermanentArchiveWrite        = "heterolog.failurearchive.write.permanent"
	SpanDelegate                     = "heterolog.failurearchive.delegate"
	SpanFilePurgeByAge               = "heterolog.failurearchive.purge.age"
	SpanFilePurgeByGeneration        = "heterolog.failurearchive.purge.generation"
	SpanFilePurgeByDirectorySize     = "heterolog.failurearchive.purge.dirsize"
)
