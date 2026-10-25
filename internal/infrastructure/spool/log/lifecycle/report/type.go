package report

type PurgeSkipReason string

const (
	PurgeSkipReasonNone PurgeSkipReason = ""

	// state 全体
	PurgeSkipReasonStateDisabled PurgeSkipReason = "state_disabled"

	// by-age
	PurgeSkipReasonAgeAlreadyRunToday PurgeSkipReason = "age_already_run_today"

	// by-size
	PurgeSkipReasonSizeIntervalNotDue PurgeSkipReason = "size_interval_not_due"
)

type RunReport struct {
	DonePurgeReport       PurgeReport
	DeadPurgeReport       PurgeReport
	QuarantinePurgeReport PurgeReport
}

// PurgeReport は、1 state に対する purge の集計です。
type PurgeReport struct {
	// SkipReason は、state 全体の purge を実行しなかった理由です。
	//
	// 空文字の場合は、state 全体の skip 理由なしです。
	// state 全体が skip された場合、ByAge / BySize は実行されません。
	SkipReason PurgeSkipReason

	// ByAge は、保持期限超過による purge 結果です。
	ByAge PurgeStepReport

	// BySize は、容量超過解消のための purge 結果です。
	BySize PurgeStepReport
}

// Skipped は、state 全体の purge が明示的に skip され、
// by-age / by-size のどちらも実行されていない場合に true を返します。
func (r PurgeReport) Skipped() bool {
	return !r.Executed() && r.SkipReason != PurgeSkipReasonNone
}

// Executed は、by-age または by-size のどちらかが実行された場合に true を返します。
func (r PurgeReport) Executed() bool {
	return r.ByAge.Executed || r.BySize.Executed
}

// HasInvalidBucketDirs は、purge 全体で不正な bucket dir があれば true を返します。
func (r PurgeReport) HasInvalidBucketDirs() bool {
	return r.ByAge.HasInvalidBucketDirs() || r.BySize.HasInvalidBucketDirs()
}

// DeletedFileCount は、purge 全体で削除した managed spool file の総数を返します。
func (r PurgeReport) DeletedFileCount() int {
	return r.ByAge.DeletedFileCount + r.BySize.DeletedFileCount
}

// DeletedTotalSize は、purge 全体で削除した managed spool file の合計サイズを返します。
func (r PurgeReport) DeletedTotalSize() uint64 {
	return r.ByAge.DeletedTotalSize + r.BySize.DeletedTotalSize
}

// RemovedEmptyDirCount は、purge 全体で削除した空 bucket dir の総数を返します。
func (r PurgeReport) RemovedEmptyDirCount() int {
	return r.ByAge.RemovedEmptyDirCount + r.BySize.RemovedEmptyDirCount
}

// PurgeStepReport は、1種類の purge 処理で得られた集計です。
type PurgeStepReport struct {
	// Executed は、この purge step の処理を実行した場合に true です。
	//
	// true でも DeletedFileCount が 0 の場合があります。
	// その場合は、処理は実行したが削除対象がなかったことを意味します。
	Executed bool

	// SkipReason は、この purge step を実行しなかった理由です。
	//
	// 空文字の場合は、skip 理由なしです。
	// Executed が false かつ SkipReason が空でない場合、
	// この step は明示的に skip されたことを意味します。
	SkipReason PurgeSkipReason

	// DeletedFileCount は、削除した managed spool file の数です。
	DeletedFileCount int

	// DeletedTotalSize は、削除した managed spool file の合計サイズです。
	DeletedTotalSize uint64

	// RemovedEmptyDirCount は、削除した空 bucket dir の数です。
	RemovedEmptyDirCount int

	// InvalidBucketDirCount は、不正な bucket dir をスキップした数です。
	InvalidBucketDirCount int

	// FirstInvalidBucketDir は、最初に検出した不正な bucket dir です。
	//
	// 全件を保持せず、ログや通知用の代表値として1件だけ保持します。
	FirstInvalidBucketDir string

	// FirstInvalidBucketDirReason は、FirstInvalidBucketDir を不正とした理由です。
	FirstInvalidBucketDirReason string
}

// Skipped は、この purge step が明示的に skip された場合に true を返します。
func (r PurgeStepReport) Skipped() bool {
	return !r.Executed && r.SkipReason != PurgeSkipReasonNone
}

// HasInvalidBucketDirs は、不正な bucket dir があれば true を返します。
func (r PurgeStepReport) HasInvalidBucketDirs() bool {
	return r.InvalidBucketDirCount > 0
}

// AddInvalidBucketDir は、不正な bucket dir を記録します。
//
// 件数は常に加算し、path/reason は最初の1件だけ保持します。
func (r *PurgeStepReport) AddInvalidBucketDir(path string, err error) {
	if r == nil {
		return
	}

	r.InvalidBucketDirCount++

	if r.FirstInvalidBucketDir != "" {
		return
	}

	r.FirstInvalidBucketDir = path
	if err != nil {
		r.FirstInvalidBucketDirReason = err.Error()
	}
}
