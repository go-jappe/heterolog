package pathidentity

// AppendUniquePaths combines paths and additional in order and returns a new
// list without empty paths and duplicates. Duplicates within paths are removed
// too, and the spelling and the order of the first occurrence of each path are
// kept. The input slices are not modified. Pass cleaned absolute paths for
// everything other than empty strings; this function neither cleans paths nor
// makes them absolute. Comparison uses the same filesystem case rules as
// ComparisonKey.
func AppendUniquePaths(paths []string, additional ...string) []string {
	result := make([]string, 0, len(paths)+len(additional))
	seen := make(map[string]struct{}, len(paths)+len(additional))
	appendPath := func(path string) {
		if path == "" {
			return
		}
		key := platformKey(path)
		if _, duplicate := seen[key]; duplicate {
			return
		}
		seen[key] = struct{}{}
		result = append(result, path)
	}
	for _, path := range paths {
		appendPath(path)
	}
	for _, path := range additional {
		appendPath(path)
	}
	return result
}
