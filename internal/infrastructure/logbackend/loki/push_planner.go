package loki

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	"github.com/go-jappe/heterolog/pkg/utils/jsonutil"
)

var (
	ErrInvalidPushLimits  = errors.New("invalid Loki push limits")
	ErrLogLineTooLarge    = errors.New("Loki log line is too large")
	ErrPushRecordTooLarge = errors.New("Loki push record is too large")
)

// PushLimitsは、applicationがLokiへ送信する一行と一requestの上限です。
// MaxPushRequestBytesはgzip圧縮前のcanonical JSON bodyに適用します。
type PushLimits struct {
	MaxLineBytes        int
	MaxPushRequestBytes int
}

// Validateは、送信前batch計画に使用できる正の上限であることを検証します。
func (l PushLimits) Validate() error {
	var errs []error
	if l.MaxLineBytes <= 0 {
		errs = append(errs, fmt.Errorf("%w: max line bytes must be positive: %d", ErrInvalidPushLimits, l.MaxLineBytes))
	}
	if l.MaxPushRequestBytes <= 0 {
		errs = append(errs, fmt.Errorf(
			"%w: max push request bytes must be positive: %d",
			ErrInvalidPushLimits,
			l.MaxPushRequestBytes,
		))
	}
	if l.MaxLineBytes > 0 && l.MaxPushRequestBytes > 0 && l.MaxLineBytes >= l.MaxPushRequestBytes {
		errs = append(errs, fmt.Errorf(
			"%w: max line bytes %d must be less than max push request bytes %d",
			ErrInvalidPushLimits,
			l.MaxLineBytes,
			l.MaxPushRequestBytes,
		))
	}
	return errors.Join(errs...)
}

// PushRecordは、送信対象一件と呼び出し元slice上の位置を保持します。
// InputIndexはplannerが事前に除外したrecordやrequest別結果を元入力へ対応付けるために使います。
type PushRecord struct {
	InputIndex int
	Labels     map[string]string
	Entry      pkgloki.Entry
}

// RejectedPushRecordは、backendへ送る前にapplication側上限を超過した一件です。
type RejectedPushRecord struct {
	Record PushRecord
	Err    error
}

// PlannedPushRequestは、上限内で一度だけPushできるrequestです。
// RecordsとStreamsは同じ入力集合を表し、Recordsは結果対応、Streamsはwire送信に使用します。
type PlannedPushRequest struct {
	Records []PushRecord
	Streams []pkgloki.Stream
}

// PushPlanは、network送信前に確定したrequest群と、送信せず除外するrecord群です。
type PushPlan struct {
	Requests []PlannedPushRequest
	Rejected []RejectedPushRecord
}

// PlanPushRequestsはline上限を超えるrecordを除外し、残りをcanonical JSON byte上限内の
// requestへ分けます。この計画を最初の送信に使用します。中継先などがより小さい上限で
// HTTP 413を返した場合、送信側は複数entryのrequestをSplitPushRequestで分けて再送します。
func PlanPushRequests(records []PushRecord, limits PushLimits) (PushPlan, error) {
	if err := limits.Validate(); err != nil {
		return PushPlan{}, err
	}

	plan := PushPlan{
		Requests: make([]PlannedPushRequest, 0, 1),
		Rejected: make([]RejectedPushRecord, 0),
	}
	candidates := make([]PushRecord, 0, len(records))
	for _, record := range records {
		// JSONは不正なUTF-8のbyteをU+FFFDに置き換えます。上限は、元のGoのbyte列ではなく、
		// Lokiが受け取るdecode後のlineに適用します。
		lineBytes := jsonutil.UTF8StringBytes(record.Entry.Line)
		if lineBytes > limits.MaxLineBytes {
			plan.Rejected = append(plan.Rejected, RejectedPushRecord{
				Record: record,
				Err: fmt.Errorf(
					"%w: input index %d has %d bytes, limit is %d",
					ErrLogLineTooLarge,
					record.InputIndex,
					lineBytes,
					limits.MaxLineBytes,
				),
			})
			continue
		}
		candidates = append(candidates, record)
	}

	for start := 0; start < len(candidates); {
		singleStreams := streamsFromPushRecords(candidates[start : start+1])
		singleSize, err := pkgloki.PushRequestSize(singleStreams)
		if err != nil {
			return PushPlan{}, fmt.Errorf("measure Loki push record at input index %d: %w", candidates[start].InputIndex, err)
		}
		if singleSize > limits.MaxPushRequestBytes {
			plan.Rejected = append(plan.Rejected, RejectedPushRecord{
				Record: candidates[start],
				Err: fmt.Errorf(
					"%w: input index %d requires %d bytes, limit is %d",
					ErrPushRecordTooLarge,
					candidates[start].InputIndex,
					singleSize,
					limits.MaxPushRequestBytes,
				),
			})
			start++
			continue
		}

		bestEnd, err := largestRequestEnd(candidates, start, limits.MaxPushRequestBytes)
		if err != nil {
			return PushPlan{}, err
		}

		requestRecords := append([]PushRecord(nil), candidates[start:bestEnd]...)
		plan.Requests = append(plan.Requests, PlannedPushRequest{
			Records: requestRecords,
			Streams: streamsFromPushRecords(requestRecords),
		})
		start = bestEnd
	}

	return plan, nil
}

// SplitPushRequestは、HTTP 413で拒否された複数entryのrequestを、計画時のrecord順を
// 保って前半と後半の二つのrequestへ分けます。413ではどのentryも受理されていないため、
// 分けたrequestを再送しても重複しません。各requestのStreamsは、そのrequestの
// Recordsだけから作り直します。一件以下のrequestは分割できないため、okはfalseです。
func SplitPushRequest(request PlannedPushRequest) (first, second PlannedPushRequest, ok bool) {
	if len(request.Records) < 2 {
		return PlannedPushRequest{}, PlannedPushRequest{}, false
	}
	middle := len(request.Records) / 2
	firstRecords := append([]PushRecord(nil), request.Records[:middle]...)
	secondRecords := append([]PushRecord(nil), request.Records[middle:]...)
	return PlannedPushRequest{Records: firstRecords, Streams: streamsFromPushRecords(firstRecords)},
		PlannedPushRequest{Records: secondRecords, Streams: streamsFromPushRecords(secondRecords)},
		true
}

// largestRequestEndはstartのrecordを含み、request上限内に収まる最大のexclusive endを
// 返します。まず1、2、4...件と探索範囲を広げるため、一件だけで上限に近い設定でも、
// 残り全件を繰り返しmarshalしません。request sizeがrecord追加に対して減らないことを
// 利用し、最初に上限を超えた範囲内だけを二分探索します。
func largestRequestEnd(records []PushRecord, start, maxRequestBytes int) (int, error) {
	bestEnd := start + 1
	upperEnd := bestEnd
	step := 1
	for bestEnd < len(records) {
		upperEnd = min(start+1+step, len(records))
		fits, err := pushRecordsFitRequestLimit(records[start:upperEnd], maxRequestBytes)
		if err != nil {
			return 0, err
		}
		if !fits {
			break
		}
		bestEnd = upperEnd
		if bestEnd == len(records) {
			return bestEnd, nil
		}
		step *= 2
	}

	low, high := bestEnd+1, upperEnd-1
	for low <= high {
		middle := low + (high-low)/2
		fits, err := pushRecordsFitRequestLimit(records[start:middle], maxRequestBytes)
		if err != nil {
			return 0, err
		}
		if fits {
			bestEnd = middle
			low = middle + 1
			continue
		}
		high = middle - 1
	}
	return bestEnd, nil
}

func pushRecordsFitRequestLimit(records []PushRecord, maxRequestBytes int) (bool, error) {
	size, err := pkgloki.PushRequestSize(streamsFromPushRecords(records))
	if err != nil {
		return false, fmt.Errorf("measure Loki push request: %w", err)
	}
	return size <= maxRequestBytes, nil
}

func streamsFromPushRecords(records []PushRecord) []pkgloki.Stream {
	streams := make([]pkgloki.Stream, 0, len(records))
	streamByKey := make(map[string]int, len(records))

	for _, record := range records {
		key := pushLabelKey(record.Labels)
		streamIndex, exists := streamByKey[key]
		if !exists {
			streamIndex = len(streams)
			streamByKey[key] = streamIndex
			streams = append(streams, pkgloki.Stream{
				Labels:  record.Labels,
				Entries: make([]pkgloki.Entry, 0, 1),
			})
		}
		streams[streamIndex].Entries = append(streams[streamIndex].Entries, record.Entry)
	}

	return streams
}

func pushLabelKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, key := range keys {
		value := labels[key]
		b.WriteString(strconv.Itoa(len(key)))
		b.WriteByte(':')
		b.WriteString(key)
		b.WriteString(strconv.Itoa(len(value)))
		b.WriteByte(':')
		b.WriteString(value)
	}
	return b.String()
}
