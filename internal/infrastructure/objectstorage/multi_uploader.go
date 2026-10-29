package objectstorage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

var _ uploader.DestinationUploader = (*MultiUploader)(nil)
var _ uploader.DestinationServiceProvider = (*MultiUploader)(nil)

// MultiUploaderは、設定順を維持した複数の物理送信先へuploadします。
// child間の送信は並行実行しますが、UploadReportは常に要求された宛先順で返します。
type MultiUploader struct {
	destinations []uploader.Destination
	byID         map[objectstorage.DestinationID]uploader.Destination
	tracer       pkgobserver.Tracer
	meter        pkgobserver.Meter
}

// NewMultiUploaderは、uploadを計装しないMultiUploaderを作ります。
func NewMultiUploader(destinations ...uploader.Destination) (*MultiUploader, error) {
	return NewMultiUploaderWithObservers(nil, nil, destinations...)
}

// NewMultiUploaderWithObserversは、宛先ごとのuploadをtracerのspanとmeterのmetricsで計装する
// MultiUploaderを作ります。nilは記録しないものとして扱います。
func NewMultiUploaderWithObservers(
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	destinations ...uploader.Destination,
) (*MultiUploader, error) {
	if len(destinations) == 0 {
		return nil, ErrDestinationsRequired
	}

	ordered := make([]uploader.Destination, 0, len(destinations))
	byID := make(map[objectstorage.DestinationID]uploader.Destination, len(destinations))
	for index, destination := range destinations {
		if strings.TrimSpace(string(destination.ID)) == "" {
			return nil, fmt.Errorf(
				"object storage destination[%d]: %w",
				index,
				uploader.ErrDestinationIDRequired,
			)
		}
		if destination.Uploader == nil {
			return nil, fmt.Errorf("object storage destination %q %w", destination.ID, ErrDestinationUploaderRequired)
		}
		if _, exists := byID[destination.ID]; exists {
			return nil, fmt.Errorf(
				"object storage destination %q: %w",
				destination.ID,
				uploader.ErrDuplicateDestinationID,
			)
		}
		ordered = append(ordered, destination)
		byID[destination.ID] = destination
	}

	return &MultiUploader{
		destinations: ordered,
		byID:         byID,
		tracer:       observer.EnsureTracer(tracer),
		meter:        observer.EnsureMeter(meter),
	}, nil
}

// DestinationIDsはconstructorで確定した宛先identityを設定順で返します。
func (m *MultiUploader) DestinationIDs() []objectstorage.DestinationID {
	if m == nil {
		return nil
	}
	ids := make([]objectstorage.DestinationID, len(m.destinations))
	for index, destination := range m.destinations {
		ids[index] = destination.ID
	}
	return ids
}

// DestinationServiceは、構築時に登録した宛先のbackendを返します。
func (m *MultiUploader) DestinationService(id objectstorage.DestinationID) (objectstorage.UploadTarget, bool) {
	if m == nil {
		return "", false
	}
	destination, exists := m.byID[id]
	return destination.Service, exists && destination.Service != ""
}

// Uploadは既存の単一error契約を維持しながら、全宛先を必ず分類します。
func (m *MultiUploader) Upload(ctx context.Context, request uploader.UploadRequest) error {
	if m == nil {
		return nil
	}
	report := m.UploadDestinations(ctx, request, m.DestinationIDs())
	return report.Err()
}

// UploadDestinationsは指定された宛先を並行して処理し、入力順に結果を返します。
// blank、duplicate、未登録のidentityも、その位置の明示的な失敗として分類します。
func (m *MultiUploader) UploadDestinations(
	ctx context.Context,
	request uploader.UploadRequest,
	destinationIDs []objectstorage.DestinationID,
) uploader.UploadReport {
	report := uploader.UploadReport{
		Results: make([]uploader.DestinationResult, len(destinationIDs)),
	}
	if err := request.Validate(); err != nil {
		for index, destinationID := range destinationIDs {
			report.Results[index] = uploader.DestinationResult{DestinationID: destinationID, Err: err}
		}
		return report
	}
	seen := make(map[objectstorage.DestinationID]struct{}, len(destinationIDs))

	var wg sync.WaitGroup
	for index, destinationID := range destinationIDs {
		report.Results[index].DestinationID = destinationID
		if strings.TrimSpace(string(destinationID)) == "" {
			report.Results[index].Err = uploader.ErrDestinationIDRequired
			continue
		}
		if _, duplicated := seen[destinationID]; duplicated {
			report.Results[index].Err = fmt.Errorf(
				"destination %q: %w",
				destinationID,
				uploader.ErrDuplicateDestinationID,
			)
			continue
		}
		seen[destinationID] = struct{}{}

		var destination uploader.Destination
		if m != nil {
			destination = m.byID[destinationID]
		}
		if destination.Uploader == nil {
			report.Results[index].Err = fmt.Errorf(
				"destination %q: %w",
				destinationID,
				uploader.ErrUnknownDestinationID,
			)
			continue
		}

		wg.Add(1)
		go func(resultIndex int, destination uploader.Destination) {
			defer wg.Done()
			report.Results[resultIndex] = m.uploadToDestination(ctx, request, destination)
		}(index, destination)
	}
	wg.Wait()
	return report
}

func (m *MultiUploader) uploadToDestination(
	ctx context.Context,
	request uploader.UploadRequest,
	destination uploader.Destination,
) (result uploader.DestinationResult) {
	result.DestinationID = destination.ID
	ctx, span := observer.EnsureTracer(m.tracer).StartSpan(ctx, SpanObjectStorageUpload,
		pkgobserver.Attr{Key: telemetry.SpanAttrTarget, Value: string(destination.Service)},
		pkgobserver.Attr{Key: telemetry.SpanAttrDestination, Value: string(destination.ID)},
	)
	started := time.Now()
	// panicをrecoverで結果へ変えた後に、確定した結果を記録します。
	defer func() {
		outcome := telemetry.OutcomeSuccess
		switch {
		case result.Err == nil:
		case result.FailureCategory == uploader.FailureCategoryPermanent:
			outcome = telemetry.OutcomePermanent
		default:
			outcome = telemetry.OutcomeRetryable
		}
		telemetry.RecordObjectStorageUpload(ctx, m.meter, string(destination.Service), outcome, time.Since(started))
		telemetry.EndSpan(span, result.Err)
	}()
	defer func() {
		if recovered := recover(); recovered != nil {
			result.Err = errors.Join(result.Err, &DestinationPanicError{
				Destination: destination.ID,
				Uploader:    destination.Uploader,
				Value:       recovered,
			})
			result.FailureCategory = uploader.FailureCategoryRetryable
		}
	}()
	result.Err = destination.Uploader.Upload(ctx, request)
	if result.Err == nil {
		return result
	}
	result.FailureCategory = uploader.FailureCategoryRetryable
	if destination.ClassifyFailure != nil {
		if category := destination.ClassifyFailure(ctx, result.Err); category.IsValid() {
			result.FailureCategory = category
		}
	}
	return result
}
