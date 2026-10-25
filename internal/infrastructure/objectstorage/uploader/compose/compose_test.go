package compose

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	objectstoragevalidation "github.com/go-jappe/heterolog/pkg/objectstorage/validation"
)

func TestFromTargetsCreatesCustomTargetUploader(t *testing.T) {
	t.Parallel()

	// 組み立て関数は対象サービスを知らず、呼び出し側が渡した builder だけで uploader を組み立てる。
	const customTarget objectstorage.UploadTarget = "custom"
	recorder := &recordingUploader{}
	builderCalled := false

	builder := func(cfg objectstorage.UploadTargetConfigProvider) ([]uploader.Destination, error) {
		_ = cfg
		builderCalled = true
		return []uploader.Destination{{ID: "custom-destination", Uploader: recorder}}, nil
	}
	builders := map[objectstorage.UploadTarget]Builder{
		customTarget: builder,
	}

	created, err := FromTargets(objectstorage.NewUploadTargetConfigAdapter(objectstorage.UploadTargetConfig{
		Enabled: true,
		Targets: []objectstorage.UploadTarget{
			customTarget,
		},
	}), builders)
	if err != nil {
		t.Fatalf("FromTargets() error = %v", err)
	}
	if !builderCalled {
		t.Fatalf("custom target builder was not called")
	}
	destinationUploader, ok := created.(uploader.DestinationUploader)
	if !ok {
		t.Fatalf("FromTargets() uploader = %T, want DestinationUploader", created)
	}
	if got, want := destinationUploader.DestinationIDs(), []objectstorage.DestinationID{"custom-destination"}; len(got) != 1 || got[0] != want[0] {
		t.Fatalf("destination ids = %v, want %v", got, want)
	}
	serviceProvider := created.(uploader.DestinationServiceProvider)
	if service, found := serviceProvider.DestinationService("custom-destination"); !found || service != customTarget {
		t.Fatalf("destination service = %q, %t; want builder target %q", service, found, customTarget)
	}

	// 生成された multi-uploader が、builder の uploader へ入力を渡すことを確認する。
	request := uploadertest.Request(t, "rotated.log.gz")
	err = created.Upload(context.Background(), request)
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	if got, want := recorder.input.FilePath(), request.FilePath(); got != want {
		t.Fatalf("uploaded file path = %q, want %q", got, want)
	}
}

func TestFromTargetsChecksDeclaredDestinationService(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		service objectstorage.UploadTarget
		valid   bool
	}{
		{"matching", "custom", true},
		{"different backend", objectstorage.UploadTargetS3, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			created, err := FromTargets(objectstorage.NewUploadTargetConfigAdapter(objectstorage.UploadTargetConfig{
				Enabled: true, Targets: []objectstorage.UploadTarget{"custom"},
			}), map[objectstorage.UploadTarget]Builder{
				"custom": func(objectstorage.UploadTargetConfigProvider) ([]uploader.Destination, error) {
					return []uploader.Destination{{ID: "opaque-id", Service: test.service, Uploader: &recordingUploader{}}}, nil
				},
			})
			if !test.valid {
				var mismatch *DestinationServiceMismatchError
				if !errors.As(err, &mismatch) || created != nil ||
					*mismatch != (DestinationServiceMismatchError{Destination: "opaque-id", Service: test.service, Target: "custom"}) {
					t.Fatalf("FromTargets = %T, %v; want mismatched service error", created, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			service, found := created.(uploader.DestinationServiceProvider).DestinationService("opaque-id")
			if !found || service != test.service {
				t.Fatalf("destination service = %q, %t; want %q", service, found, test.service)
			}
		})
	}
}

func TestFromTargetsDisabledReturnsNil(t *testing.T) {
	t.Parallel()

	created, err := FromTargets(objectstorage.NewUploadTargetConfigAdapter(objectstorage.UploadTargetConfig{
		Enabled: false,
	}), nil)
	if err != nil {
		t.Fatalf("FromTargets() error = %v", err)
	}
	if created != nil {
		t.Fatalf("FromTargets() uploader = %T, want nil", created)
	}
}

func TestFromTargetsRejectsInvalidTargetsBeforeBuilding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		targets []objectstorage.UploadTarget
		wantErr error
		// wantTargetErrは、表記・一意性の規則に反したtargetの位置と値です。
		wantTargetErr *objectstoragevalidation.UploadTargetError
		// wantUnsupportedは、対応するbuilderのないtargetです。
		wantUnsupported objectstorage.UploadTarget
	}{
		{
			name:    "empty",
			wantErr: ErrUploadTargetsRequired,
		},
		{
			name:    "blank",
			targets: []objectstorage.UploadTarget{" "},
			wantErr: objectstoragevalidation.ErrUploadTargetRequired,
			wantTargetErr: &objectstoragevalidation.UploadTargetError{
				Index: 0, Target: " ", Err: objectstoragevalidation.ErrUploadTargetRequired,
			},
		},
		{
			name:            "unsupported",
			targets:         []objectstorage.UploadTarget{"unsupported"},
			wantErr:         objectstoragevalidation.ErrUnsupportedUploadTarget,
			wantUnsupported: "unsupported",
		},
		{
			name:    "surrounding whitespace",
			targets: []objectstorage.UploadTarget{" custom "},
			wantErr: objectstoragevalidation.ErrNonCanonicalUploadTarget,
			wantTargetErr: &objectstoragevalidation.UploadTargetError{
				Index: 0, Target: " custom ", Canonical: "custom", Err: objectstoragevalidation.ErrNonCanonicalUploadTarget,
			},
		},
		{
			name:    "uppercase",
			targets: []objectstorage.UploadTarget{"CUSTOM"},
			wantErr: objectstoragevalidation.ErrNonCanonicalUploadTarget,
			wantTargetErr: &objectstoragevalidation.UploadTargetError{
				Index: 0, Target: "CUSTOM", Canonical: "custom", Err: objectstoragevalidation.ErrNonCanonicalUploadTarget,
			},
		},
		{
			name:    "duplicate",
			targets: []objectstorage.UploadTarget{"custom", "custom"},
			wantErr: objectstoragevalidation.ErrDuplicateUploadTarget,
			wantTargetErr: &objectstoragevalidation.UploadTargetError{
				Index: 1, Target: "custom", Err: objectstoragevalidation.ErrDuplicateUploadTarget,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			builderCalls := 0
			builder := func(cfg objectstorage.UploadTargetConfigProvider) ([]uploader.Destination, error) {
				_ = cfg
				builderCalls++
				return []uploader.Destination{{ID: "custom-destination", Uploader: &recordingUploader{}}}, nil
			}
			builders := map[objectstorage.UploadTarget]Builder{
				"custom":   builder,
				" custom ": builder,
				"CUSTOM":   builder,
			}

			created, err := FromTargets(objectstorage.NewUploadTargetConfigAdapter(objectstorage.UploadTargetConfig{
				Enabled: true,
				Targets: tt.targets,
			}), builders)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("FromTargets() error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantUnsupported != "" {
				var targetErr *objectstoragevalidation.UnsupportedUploadTargetError
				if !errors.As(err, &targetErr) || targetErr.Target != tt.wantUnsupported {
					t.Fatalf("FromTargets() error = %v, want unsupported target %q", err, tt.wantUnsupported)
				}
			}
			if tt.wantTargetErr != nil {
				var targetErr *objectstoragevalidation.UploadTargetError
				if !errors.As(err, &targetErr) || *targetErr != *tt.wantTargetErr {
					t.Fatalf("FromTargets() error = %v, want %+v", err, *tt.wantTargetErr)
				}
			}
			if created != nil {
				t.Fatalf("FromTargets() uploader = %T, want nil", created)
			}
			if builderCalls != 0 {
				t.Fatalf("builder calls = %d, want 0", builderCalls)
			}
		})
	}
}

func TestFromTargetsRejectsEmptyBuilders(t *testing.T) {
	t.Parallel()

	created, err := FromTargets(objectstorage.NewUploadTargetConfigAdapter(objectstorage.UploadTargetConfig{
		Enabled: true,
		Targets: []objectstorage.UploadTarget{
			"custom",
		},
	}), nil)
	if !errors.Is(err, ErrUploaderBuildersRequired) {
		t.Fatalf("FromTargets() error = %v, want empty builders error", err)
	}
	if created != nil {
		t.Fatalf("FromTargets() uploader = %T, want nil", created)
	}
}

func TestFromTargetsRejectsNilBuilder(t *testing.T) {
	t.Parallel()

	const customTarget objectstorage.UploadTarget = "custom"

	created, err := FromTargets(objectstorage.NewUploadTargetConfigAdapter(objectstorage.UploadTargetConfig{
		Enabled: true,
		Targets: []objectstorage.UploadTarget{
			customTarget,
		},
	}), map[objectstorage.UploadTarget]Builder{
		customTarget: nil,
	})
	if !errors.Is(err, ErrUploaderBuilderRequired) {
		t.Fatalf("FromTargets() error = %v, want nil builder error", err)
	}
	if created != nil {
		t.Fatalf("FromTargets() uploader = %T, want nil", created)
	}
}

func TestFromTargetsWrapsBuilderErrorWithTarget(t *testing.T) {
	t.Parallel()

	const customTarget objectstorage.UploadTarget = "custom"
	wantErr := errors.New("create failed")
	builder := func(cfg objectstorage.UploadTargetConfigProvider) ([]uploader.Destination, error) {
		_ = cfg
		return []uploader.Destination{{}}, wantErr
	}

	created, err := FromTargets(objectstorage.NewUploadTargetConfigAdapter(objectstorage.UploadTargetConfig{
		Enabled: true,
		Targets: []objectstorage.UploadTarget{
			customTarget,
		},
	}), map[objectstorage.UploadTarget]Builder{
		customTarget: builder,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("FromTargets() error = %v, want wrapped %v", err, wantErr)
	}
	var builderErr *BuilderError
	if !errors.As(err, &builderErr) || builderErr.Target != customTarget {
		t.Fatalf("FromTargets() error = %v, want builder error for %q", err, customTarget)
	}
	if created != nil {
		t.Fatalf("FromTargets() uploader = %T, want nil", created)
	}
}

func TestFromTargetsRejectsAllNilUploaders(t *testing.T) {
	t.Parallel()

	const customTarget objectstorage.UploadTarget = "custom"
	builder := func(cfg objectstorage.UploadTargetConfigProvider) ([]uploader.Destination, error) {
		_ = cfg
		return []uploader.Destination{{}}, nil
	}

	created, err := FromTargets(objectstorage.NewUploadTargetConfigAdapter(objectstorage.UploadTargetConfig{
		Enabled: true,
		Targets: []objectstorage.UploadTarget{
			customTarget,
		},
	}), map[objectstorage.UploadTarget]Builder{
		customTarget: builder,
	})
	if !errors.Is(err, ErrUploaderRequired) {
		t.Fatalf("FromTargets() error = %v, want no uploaders error", err)
	}
	if created != nil {
		t.Fatalf("FromTargets() uploader = %T, want nil", created)
	}
}

func TestFromTargetsRejectsBlankOrDuplicateDestinationIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		targets  []objectstorage.UploadTarget
		builders map[objectstorage.UploadTarget]Builder
		wantErr  error
	}{
		{
			name:    "blank destination id",
			targets: []objectstorage.UploadTarget{"first"},
			builders: map[objectstorage.UploadTarget]Builder{
				"first": func(objectstorage.UploadTargetConfigProvider) ([]uploader.Destination, error) {
					return []uploader.Destination{{Uploader: &recordingUploader{}}}, nil
				},
			},
			wantErr: uploader.ErrDestinationIDRequired,
		},
		{
			name:    "duplicate destination id",
			targets: []objectstorage.UploadTarget{"first", "second"},
			builders: map[objectstorage.UploadTarget]Builder{
				"first": func(objectstorage.UploadTargetConfigProvider) ([]uploader.Destination, error) {
					return []uploader.Destination{{ID: "same", Uploader: &recordingUploader{}}}, nil
				},
				"second": func(objectstorage.UploadTargetConfigProvider) ([]uploader.Destination, error) {
					return []uploader.Destination{{ID: "same", Uploader: &recordingUploader{}}}, nil
				},
			},
			wantErr: uploader.ErrDuplicateDestinationID,
		},
	}

	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			created, err := FromTargets(objectstorage.NewUploadTargetConfigAdapter(objectstorage.UploadTargetConfig{
				Enabled: true,
				Targets: testCase.targets,
			}), testCase.builders)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("FromTargets() error = %v, want %v", err, testCase.wantErr)
			}
			if created != nil {
				t.Fatalf("FromTargets() uploader = %T, want nil", created)
			}
		})
	}
}

type recordingUploader struct {
	input uploader.UploadRequest
}

func (r *recordingUploader) Upload(ctx context.Context, input uploader.UploadRequest) error {
	_ = ctx
	r.input = input
	return nil
}
