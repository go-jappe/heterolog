package uploader

import (
	"context"
	"errors"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

var (
	// ErrDestinationIDRequired reports that a destination identity is empty or
	// blank.
	ErrDestinationIDRequired = errors.New("object storage destination id is required")
	// ErrDuplicateDestinationID reports that the same destination identity was
	// specified more than once.
	ErrDuplicateDestinationID = errors.New("duplicate object storage destination id")
	// ErrUnknownDestinationID reports a destination that is not registered with the
	// fan-out uploader.
	ErrUnknownDestinationID = errors.New("unknown object storage destination id")
)

// Uploader accepts a source file, decides the object name for each destination,
// and uploads the file.
type Uploader interface {
	Upload(ctx context.Context, request UploadRequest) error
}

// Destination associates the stable identity of one physical destination with
// its upload implementation.
type Destination struct {
	ID       objectstorage.DestinationID
	Uploader Uploader
	// Service is the backend of this destination. It is used to build the fallback
	// storage and the resend adapter. The backend's builder sets it instead of
	// inferring it from the destination ID.
	Service objectstorage.UploadTarget
	// ClassifyFailure classifies failures of this destination by backend-specific
	// rules. If it is not set or does not return a valid category, the failure is
	// treated as retryable.
	ClassifyFailure func(context.Context, error) FailureCategory
}

// DestinationServiceProvider is the contract for building the fallback storage
// and the resend adapter of each destination. DestinationService returns false
// for a destination that is not registered or whose backend is not set.
type DestinationServiceProvider interface {
	DestinationService(objectstorage.DestinationID) (objectstorage.UploadTarget, bool)
}

// DestinationResult is the upload result for one destination. A nil Err means
// success, and a non-nil Err means failure. Because it does not hold a status
// and an error that could contradict each other, the checkpoint side can
// interpret the result unambiguously. An uploader with built-in saving on
// upload failure also returns success once the file has been saved to every
// specified fallback. In that case, a nil Err does not necessarily mean that
// the upload to object storage succeeded.
type DestinationResult struct {
	DestinationID objectstorage.DestinationID
	Err           error
	// FailureCategory is the backend-specific category when Err is non-nil. A
	// failure from an upload implementation that has no classification is also
	// treated as retryable.
	FailureCategory FailureCategory
}

func (r DestinationResult) Succeeded() bool {
	return r.Err == nil
}

// UploadReport holds the result for each requested destination, in request
// order.
type UploadReport struct {
	Results []DestinationResult
}

// Err joins a *DestinationUploadError for each failed destination, in request
// order, with errors.Join. It returns nil if there are no failures.
func (r UploadReport) Err() error {
	errs := make([]error, 0, len(r.Results))
	for _, result := range r.Results {
		if result.Err == nil {
			continue
		}
		errs = append(errs, &DestinationUploadError{Destination: result.DestinationID, Err: result.Err})
	}
	return errors.Join(errs...)
}

// DestinationUploader is a fan-out uploader that can upload to and classify
// multiple destinations individually. Upload is the compatibility entry point
// for all destinations, and UploadDestinations handles only the specified
// destinations.
type DestinationUploader interface {
	Uploader
	DestinationIDs() []objectstorage.DestinationID
	UploadDestinations(
		ctx context.Context,
		request UploadRequest,
		destinationIDs []objectstorage.DestinationID,
	) UploadReport
}
