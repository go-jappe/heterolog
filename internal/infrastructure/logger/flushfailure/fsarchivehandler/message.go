package fsarchivehandler

const (
	ErrorMsgInvalidConfigStatus      = "invalid config status :%w"
	ErrorMsgOpenFile                 = "failed to open file; err: %w"
	ErrorMsgFileCompress             = "failed to compress failure archive: %s"
	ErrorMsgRawArchiveRecovery       = "failed to recover incomplete compressed failure archive: %s"
	ErrorMsgCompressionSourceCleanup = "failed to remove failure archive source after compression: %s"
	ErrorMsgFileArchiveUpload        = "failed to upload failure archive: %s"
	ErrorMsgUploadedArchiveDelete    = "failed to delete uploaded compressed failure archive: %s"
	ErrorMsgRotationArchivePanic     = "failure archive rotation task panicked: %s"
	ErrorMsgCreateArchiveUploader    = "failed to create %s failure archive uploader: %w"

	MsgInvalidDirectoryPath                        = "failed to retrieve directory;dir:%s  error: %w"
	MsgInvalidFileIndex                            = "failed to retrieve file index;dir:%s error: %w"
	MsgOldArchivesDeleted                          = "deleted old failure archives: %s"
	WarnMsgActiveArchiveFilesExceedMaxDirSize      = "active failure archive files alone exceed maximum directory size"
	WarnMsgNoArchivesEligibleForDirectorySizePurge = "failure archive directory size remains over limit because no files are eligible for purge"

	DebugMsgPurgedFileAge         = "failure archive file has purged by age"
	DebugMsgPurgedFileGenerations = "failure archive file has purged by generations"
	DebugMsgPurgedFileDirSize     = "failure archive file has purged by max directory size exceeded"

	OperationCompress              = "jsonl compress error"
	OperationSourceCleanup         = "compressed jsonl source cleanup error"
	OperationArchiveUpload         = "jsonl archive upload error"
	OperationUploadedArchiveDelete = "uploaded compressed jsonl archive delete error"
	OperationRotationArchive       = "failure archive rotation task error"
)
