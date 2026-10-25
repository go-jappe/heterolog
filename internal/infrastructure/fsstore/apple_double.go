package fsstore

import (
	"os"
	"path/filepath"
	"strings"
)

// appleDoublePrefixは、AppleDoubleの付随fileの名前の接頭辞です。macOSは拡張属性を保存
// できないfilesystem（FAT32・ExFATなど）で、fileやdirectoryの拡張属性を同じdirectoryの
// 「._」+対象の名前のfileへ保存し、対象のrenameや削除に合わせて移動・削除します。
const appleDoublePrefix = "._"

// IsAppleDoubleCompanionは、directory直下のnameが、同じdirectoryに対象が存在する
// AppleDoubleの付随fileかを返します。macOS以外と、対象が存在しない「._」で始まる名前では
// falseを返し、通常のentryとして扱わせます。一覧を分割して読む走査で、読込済みの範囲の外にある
// 対象も確かめるために使います。
func IsAppleDoubleCompanion(directory, name string) bool {
	owner, ok := appleDoubleOwnerName(name)
	if !ok {
		return false
	}
	_, err := os.Lstat(filepath.Join(directory, owner))
	return err == nil
}

// appleDoubleOwnerNameは、macOSでnameがAppleDoubleの付随fileの名前の形なら、対象の名前とtrueを
// 返します。
func appleDoubleOwnerName(name string) (string, bool) {
	if !hidesAppleDoubleCompanions || len(name) <= len(appleDoublePrefix) || !strings.HasPrefix(name, appleDoublePrefix) {
		return "", false
	}
	return name[len(appleDoublePrefix):], true
}

// withoutAppleDoubleCompanionsは、同じ一覧に対象が含まれるAppleDoubleの付随fileをentriesから
// 除きます。付随fileを除いても対象は一覧に残るため、directoryが空かの判定は変わりません。
func withoutAppleDoubleCompanions(entries []os.DirEntry) []os.DirEntry {
	if !hidesAppleDoubleCompanions {
		return entries
	}
	names := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		names[entry.Name()] = struct{}{}
	}
	kept := entries[:0]
	for _, entry := range entries {
		if owner, ok := appleDoubleOwnerName(entry.Name()); ok {
			if _, listed := names[owner]; listed {
				continue
			}
		}
		kept = append(kept, entry)
	}
	return kept
}
