package compose

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestDestinationIDsFromTargetsPreservesOrderWithoutBuildingUploaders(t *testing.T) {
	cfg := objectstorage.UploadTargetConfig{
		Enabled: true, Targets: []objectstorage.UploadTarget{"second", "first"},
		S3: objectstorage.S3Config{Bucket: "settings-passed-to-resolver"},
	}
	provider := objectstorage.NewUploadTargetConfigAdapter(cfg)
	var visited []objectstorage.UploadTarget
	resolvers := make(map[objectstorage.UploadTarget]DestinationIDResolver)
	for _, target := range cfg.Targets {
		resolvers[target] = func(got objectstorage.UploadTargetConfigProvider) ([]objectstorage.DestinationID, error) {
			require.Equal(t, provider, got)
			visited = append(visited, target)
			return []objectstorage.DestinationID{objectstorage.DestinationID(target + "-id")}, nil
		}
	}
	ids, err := DestinationIDsFromTargets(provider, resolvers)
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{"second-id", "first-id"}, ids)
	require.Equal(t, cfg.Targets, visited)
	ids[0] = "caller-owned"
	got, err := DestinationIDsFromTargets(provider, resolvers)
	require.NoError(t, err)
	require.Equal(t, objectstorage.DestinationID("second-id"), got[0])
}

func TestDestinationIDsFromTargetsValidatesSettingsAndResolvedIDs(t *testing.T) {
	resolveErr := errors.New("invalid provider configuration")
	valid := func(objectstorage.UploadTargetConfigProvider) ([]objectstorage.DestinationID, error) {
		return []objectstorage.DestinationID{"id"}, nil
	}
	for _, tc := range []struct {
		name      string
		targets   []objectstorage.UploadTarget
		resolvers map[objectstorage.UploadTarget]DestinationIDResolver
		wantErr   error
	}{
		{name: "no targets"},
		{name: "noncanonical target", targets: []objectstorage.UploadTarget{"S3"}},
		{name: "duplicate target", targets: []objectstorage.UploadTarget{"s3", "s3"}},
		{name: "missing resolver", targets: []objectstorage.UploadTarget{"s3"}},
		{name: "nil resolver", targets: []objectstorage.UploadTarget{"s3"}, resolvers: map[objectstorage.UploadTarget]DestinationIDResolver{"s3": nil}},
		{name: "invalid provider", targets: []objectstorage.UploadTarget{"s3"}, wantErr: resolveErr,
			resolvers: map[objectstorage.UploadTarget]DestinationIDResolver{"s3": func(objectstorage.UploadTargetConfigProvider) ([]objectstorage.DestinationID, error) {
				return nil, resolveErr
			}}},
		{name: "empty ID", targets: []objectstorage.UploadTarget{"s3"}, wantErr: uploader.ErrDestinationIDRequired,
			resolvers: map[objectstorage.UploadTarget]DestinationIDResolver{"s3": func(objectstorage.UploadTargetConfigProvider) ([]objectstorage.DestinationID, error) {
				return []objectstorage.DestinationID{" "}, nil
			}}},
		{name: "duplicate ID", targets: []objectstorage.UploadTarget{"s3", "other"}, wantErr: uploader.ErrDuplicateDestinationID,
			resolvers: map[objectstorage.UploadTarget]DestinationIDResolver{"s3": valid, "other": valid}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ids, err := DestinationIDsFromTargets(objectstorage.NewUploadTargetConfigAdapter(objectstorage.UploadTargetConfig{Enabled: true, Targets: tc.targets}), tc.resolvers)
			require.Error(t, err)
			require.Nil(t, ids)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			}
		})
	}
}

func TestDestinationIDsFromTargetsDisabledDoesNotResolve(t *testing.T) {
	ids, err := DestinationIDsFromTargets(objectstorage.NewUploadTargetConfigAdapter(objectstorage.UploadTargetConfig{Targets: []objectstorage.UploadTarget{"s3"}}),
		map[objectstorage.UploadTarget]DestinationIDResolver{"s3": func(objectstorage.UploadTargetConfigProvider) ([]objectstorage.DestinationID, error) {
			t.Fatal("disabled upload must not resolve or validate provider settings")
			return nil, nil
		}})
	require.NoError(t, err)
	require.Nil(t, ids)
}
