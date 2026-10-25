package compose

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

var (
	// ErrUploadTargetsRequiredは、有効なupload設定にtargetが一つもないことを表します。
	ErrUploadTargetsRequired = errors.New("object storage upload targets are required")
	// ErrUploaderBuildersRequiredは、uploaderを組み立てるbuilderが一つも渡されなかったことを表します。
	ErrUploaderBuildersRequired = errors.New("object storage uploader builders are required")
	// ErrUploaderBuilderRequiredは、targetに対応するbuilderがnilであることを表します。
	ErrUploaderBuilderRequired = errors.New("object storage uploader builder is required")
	// ErrUploaderRequiredは、builderがtargetのuploaderを返さなかったか、nilのuploaderを返したことを表します。
	ErrUploaderRequired = errors.New("object storage uploader is required")
)

// BuilderErrorは、Targetのbuilderがuploaderを作成できなかったことを示します。Errはbuilderのerrorです。
type BuilderError struct {
	Target objectstorage.UploadTarget
	Err    error
}

func (e *BuilderError) Error() string {
	return fmt.Sprintf("failed to create %s object storage uploader: %v", e.Target, e.Err)
}

func (e *BuilderError) Unwrap() error {
	return e.Err
}

// DestinationServiceMismatchErrorは、Targetのbuilderが別のserviceの宛先を返したことを示します。
type DestinationServiceMismatchError struct {
	Destination objectstorage.DestinationID
	Service     objectstorage.UploadTarget
	Target      objectstorage.UploadTarget
}

func (e *DestinationServiceMismatchError) Error() string {
	return fmt.Sprintf(
		"object storage destination %q service %q does not match builder target %q",
		e.Destination, e.Service, e.Target,
	)
}
