package pathidentity

import "golang.org/x/sys/unix"

// Darwinの_PC_CASE_SENSITIVEは、Appleが公開しているsys/unistd.hで宣言されています:
// https://github.com/apple-oss-distributions/xnu/blob/main/bsd/sys/unistd.h
// すべてのUnixのvolumeが大小文字を区別すると決めつけず、filesystemに問い合わせます。
func filesystemCaseSensitive(directory string) (bool, error) {
	const pcCaseSensitive = 11
	value, err := unix.Pathconf(directory, pcCaseSensitive)
	return value != 0, err
}

func filesystemPathKey(path string) string {
	return caseKey(path, filesystemCaseSensitive)
}

// filesystemDirectoryNameCaseはComparisonKeyと同じ問い合わせで、directory直下の規則を返します。
func filesystemDirectoryNameCase(directory string) NameCase {
	return directoryNameCase(directory, filesystemCaseSensitive)
}
