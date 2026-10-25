package objectstorage

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

var (
	// ErrDestinationsRequiredは、MultiUploaderへ宛先が一つも渡されなかったことを表します。
	ErrDestinationsRequired = errors.New("object storage destinations are required")
	// ErrDestinationUploaderRequiredは、宛先のuploaderがnilであることを表します。
	ErrDestinationUploaderRequired = errors.New("uploader is required")
)

// DestinationPanicErrorは、宛先のuploaderのpanicを回復したことを示します。
type DestinationPanicError struct {
	Destination objectstorage.DestinationID
	Uploader    uploader.Uploader
	// Valueは回復したpanicの値です。
	Value any
}

func (e *DestinationPanicError) Error() string {
	return fmt.Sprintf("object storage destination %q uploader (%T) panic: %v", e.Destination, e.Uploader, e.Value)
}
