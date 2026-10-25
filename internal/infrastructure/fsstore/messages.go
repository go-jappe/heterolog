package fsstore

const (
	MsgRotationAges              = "file rotation triggered: file exceeded the configured retention period"
	MsgRotationDirSize           = "file rotation triggered: max directory size exceeded"
	MsgRotationExceeded          = "file rotation triggered: number of files exceeded the configured retention limit."
	DebugMsgFileAgePurgeNeverRun = "file age-based purge has never run since startup"
	MsgOsStatError               = "failed to find file :%s,"
	MsgFailedDeleteFile          = "failed to delete file: %s"
)
