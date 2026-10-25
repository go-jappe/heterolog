//go:build !darwin

package fsstore

// macOS以外では、「._」で始まる名前も通常のentryとして扱います。
const hidesAppleDoubleCompanions = false
