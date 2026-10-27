// Package failurestore defines the durable store selection values shared by
// failure pipelines with different payload formats.
package failurestore

// DurableType identifies where failed data is persisted. The settings and the
// write contract of each storage location are defined by payload-specific
// packages; this type represents only the common selection value.
type DurableType string

const (
	// DurableTypeNone explicitly selects no durable failure storage.
	DurableTypeNone DurableType = "none"
	// DurableTypeFileArchive stores failed data in a filesystem archive.
	DurableTypeFileArchive DurableType = "fileArchive"
	// DurableTypeSpool stores failed data in a spool for retries.
	DurableTypeSpool DurableType = "spool"
)
