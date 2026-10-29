package compose

import (
	"fmt"

	infraobjectstorage "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	objectstoragevalidation "github.com/go-jappe/heterolog/pkg/objectstorage/validation"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// Builderは一つのproviderについて、設定された全宛先のuploaderを生成します。
type Builder func(cfg objectstorage.UploadTargetConfigProvider) ([]uploader.Destination, error)

// Optionは、FromTargetsが組み立てるuploaderの設定です。
type Option func(*options)

type options struct {
	tracer pkgobserver.Tracer
	meter  pkgobserver.Meter
}

// WithObserversは、宛先ごとのuploadをtracerのspanとmeterのmetricsで計装します。
func WithObservers(tracer pkgobserver.Tracer, meter pkgobserver.Meter) Option {
	return func(o *options) {
		o.tracer = tracer
		o.meter = meter
	}
}

// FromTargets は設定された upload target に対応する uploader を組み立てます。
func FromTargets(
	cfg objectstorage.UploadTargetConfigProvider,
	builders map[objectstorage.UploadTarget]Builder,
	opts ...Option,
) (uploader.DestinationUploader, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if !cfg.Enabled() {
		return nil, nil
	}
	targets := cfg.Targets()
	if len(targets) == 0 {
		return nil, ErrUploadTargetsRequired
	}
	if err := objectstoragevalidation.ValidateUploadTargets(targets); err != nil {
		return nil, err
	}
	if len(builders) == 0 {
		return nil, ErrUploaderBuildersRequired
	}

	destinations := make([]uploader.Destination, 0, len(targets))
	for _, target := range targets {
		builder, ok := builders[target]
		if !ok {
			return nil, &objectstoragevalidation.UnsupportedUploadTargetError{Target: target}
		}
		if builder == nil {
			return nil, fmt.Errorf("%w for target %q", ErrUploaderBuilderRequired, target)
		}
		built, err := builder(cfg)
		if err != nil {
			return nil, &BuilderError{Target: target, Err: err}
		}
		if len(built) == 0 {
			return nil, fmt.Errorf("%w for target %q", ErrUploaderRequired, target)
		}
		for _, destination := range built {
			if destination.Uploader == nil {
				return nil, fmt.Errorf("%w for target %q", ErrUploaderRequired, target)
			}
			if destination.Service != "" && destination.Service != target {
				return nil, &DestinationServiceMismatchError{Destination: destination.ID, Service: destination.Service, Target: target}
			}
			destination.Service = target
			destinations = append(destinations, destination)
		}
	}
	multi, err := infraobjectstorage.NewMultiUploaderWithObservers(o.tracer, o.meter, destinations...)
	if err != nil {
		return nil, err
	}
	return multi, nil
}
