package lifecycle

// fileGenerationはfile名に含まれる日付と番号です。処理順序と成功記録のkeyにだけ使い、
// 宛先ごとの処理位置としては保持しません。
type fileGeneration struct {
	Period string
	Index  uint64
}

func (g fileGeneration) compare(other fileGeneration) int {
	if g.Period < other.Period || g.Period == other.Period && g.Index < other.Index {
		return -1
	}
	if g.Period == other.Period && g.Index == other.Index {
		return 0
	}
	return 1
}
