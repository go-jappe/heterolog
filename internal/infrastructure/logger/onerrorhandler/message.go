package onerrorhandler

const (
	MismatchLevel = "Level mismatch: the entry level is %v, but the claim level is %v"
	// MissingEntryErrorMessageは、entryなしで渡されたerrorを通知する内部ERROR logの本文です。
	MissingEntryErrorMessage = "internal error reported without log entry"
)
