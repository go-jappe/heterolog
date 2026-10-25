package pathidentity

import "golang.org/x/sys/unix"

// Darwin _PC_CASE_SENSITIVE is declared in Apple's public sys/unistd.h:
// https://github.com/apple-oss-distributions/xnu/blob/main/bsd/sys/unistd.h
// Query the filesystem rather than assuming every Unix volume is case-sensitive.
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
