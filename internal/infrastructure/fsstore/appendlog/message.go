package appendlog

const (
	ErrorMesFailedCreateRotationFilePath = "failed to create rotation log file path"
	ErrorMesFailedFileClose              = "failed to close log file; path: %v"
	ErrorMesFailedFileOpen               = "failed to open log file; path: %v"
	ErrFailedOpenFile                    = "failed to open log file; path%v; err: %w"

	DebugMsgStartFileAgePurge = "starting age-based file purge"

	WarnMsgFileDirSizePurgeStart = "log file purge was triggered due to maximum directory size; exceed size %v; maximum directory size %v"
	InfoMsgFileDirSizePurgeDone  = "log file purge has completed due to maximum directory size"
)
