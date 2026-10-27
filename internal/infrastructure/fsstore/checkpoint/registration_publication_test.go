package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/stretchr/testify/require"
)

func TestRegistrationPublicationFailurePreservesEligibilityAcrossRetryAndRestart(t *testing.T) {
	for _, published := range []bool{false, true} {
		for _, restart := range []bool{false, true} {
			name := "before-publication/same-process"
			if published {
				name = "after-publication/same-process"
			}
			if restart {
				name += "/restart"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx := context.Background()
					store, err := NewFilesystemStore(t.TempDir(), "source")
					require.NoError(t, err)
					defer func() {
						if store.PendingSaveError(ctx) != nil {
							_ = store.ForceClose()
						}
						_ = boundedwait.CloseInBubble(t, "store.Close()", store.Close, store.ForceClose)
					}()
					failure := errors.New("registration directory sync failed")
					var restore func()
					if published {
						setCheckpointDirectorySyncForTest(store, func(string) error { return failure })
						restore = func() { setCheckpointDirectorySyncForTest(store, infrafsstore.SyncDirectory) }
					} else {
						// UnixでもWindowsでも、空でないdirectoryは公開を拒否します。
						backup := filepath.Join(t.TempDir(), "registrations.json")
						require.NoError(t, os.Rename(store.registrationsPath(), backup))
						require.NoError(t, os.Mkdir(store.registrationsPath(), 0o700))
						marker := filepath.Join(store.registrationsPath(), "preserve")
						require.NoError(t, os.WriteFile(marker, []byte("preserve"), fileMode))
						restore = func() {
							require.NoError(t, os.Remove(marker))
							require.NoError(t, os.Remove(store.registrationsPath()))
							require.NoError(t, os.Rename(backup, store.registrationsPath()))
						}
					}
					ids := []objectstorage.DestinationID{"primary"}
					err = store.RegisterDestinations(ctx, ids, nil)
					require.Error(t, err)
					require.False(t, IsPendingSaveOnly(err), "the requested initial cursor has not been accepted")
					if published {
						require.ErrorIs(t, err, failure)
						assertPublishedRegistrationIDs(t, store, ids)
						require.ErrorIs(t, store.PendingSaveError(ctx), failure)
					} else {
						// 空でないdirectoryが宛先登録文書の公開(rename)を拒否した失敗です。
						var publishErr *os.LinkError
						require.ErrorAs(t, err, &publishErr)
						require.Equal(t, store.registrationsPath(), publishErr.New)
						require.NoError(t, store.PendingSaveError(ctx))
					}
					restore()
					// この世代は、登録を試みた後に現れたものです。
					newKey := CheckpointKey{Generation: "20261021-1", Compressed: true}
					if restart {
						if published {
							require.Error(t, store.ForceClose())
							require.ErrorIs(t, boundedwait.CloseInBubble(t, "store.Close()", store.Close, store.ForceClose), failure)
						} else {
							require.NoError(t, boundedwait.CloseInBubble(t, "store.Close()", store.Close, store.ForceClose))
						}
						store, err = NewFilesystemStore(store.Directory(), "source")
						require.NoError(t, err)
					} else if published {
						// I/Oを復旧しただけでは、recoveryがmanifestを同期する前に新しいcursorを認めません。
						err = store.RegisterDestinations(ctx, ids, []CheckpointKey{newKey})
						require.ErrorIs(t, err, failure)
						require.False(t, IsPendingSaveOnly(err))
						advanceCheckpointRecoveryForTest()
						require.NoError(t, store.PendingSaveError(ctx))
					}
					require.NoError(t, store.RegisterDestinations(ctx, ids, []CheckpointKey{newKey}))
					snapshot, err := store.Load(ctx, newKey)
					require.NoError(t, err)
					if published {
						require.Equal(t, ids, snapshot.PendingDestinationIDs(ids), "published IDs must not receive a new exclusion boundary")
					} else {
						require.True(t, snapshot.Complete(ids), "an unpublished registration retains the existing first-registration policy")
					}
					require.NoError(t, boundedwait.CloseInBubble(t, "store.Close()", store.Close, store.ForceClose))
					strict, err := NewFilesystemStore(store.Directory(), "source", WithMissingCheckpointRecovery(false))
					require.NoError(t, err, "successful recovery must persist both registration and cursor documents")
					defer func() {
						require.NoError(t, boundedwait.CloseInBubble(t, "strict.Close()", strict.Close, strict.ForceClose))
					}()
					reloaded, err := strict.Load(ctx, newKey)
					require.NoError(t, err)
					require.Equal(t, snapshot, reloaded)
				})
			})
		}
	}
}

func TestPublishedRegistrationRecoveryDoesNotAcceptFailedCursorOrProgressUpdates(t *testing.T) {
	for _, operation := range []string{"register", "advance", "progress"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				store := newCheckpointTestStore(t, "source")
				defer func() {
					if store.PendingSaveError(ctx) != nil {
						_ = store.ForceClose()
					}
				}()
				key := CheckpointKey{Generation: "20261021-1", Compressed: true}
				ids := []objectstorage.DestinationID{"primary"}
				failure := errors.New("registration sync unavailable")
				setCheckpointDirectorySyncForTest(store, func(string) error { return failure })
				update := func() error {
					switch operation {
					case "register":
						return store.RegisterDestinations(ctx, ids, []CheckpointKey{key})
					case "advance":
						_, err := store.AdvanceCompletionCursor(ctx, key, ids)
						return err
					default:
						return store.SaveUploadProgress(ctx, UploadProgress{Key: key, DestinationID: ids[0], UploadResult: UploadResultSuccess})
					}
				}
				for range 2 {
					err := update()
					require.ErrorIs(t, err, failure)
					require.False(t, IsPendingSaveOnly(err), "a failed update must not look accepted to the caller")
					snapshot, err := store.Load(ctx, key)
					require.NoError(t, err)
					require.Equal(t, ids, snapshot.PendingDestinationIDs(ids))
					_, found, err := store.LoadUploadProgress(ctx, key, ids[0])
					require.NoError(t, err)
					require.False(t, found)
					advanceCheckpointRecoveryForTest()
				}
				setCheckpointDirectorySyncForTest(store, infrafsstore.SyncDirectory)
				advanceCheckpointRecoveryForTest()
				require.NoError(t, store.PendingSaveError(ctx))
				snapshot, err := store.Load(ctx, key)
				require.NoError(t, err)
				require.Equal(t, ids, snapshot.PendingDestinationIDs(ids), "background recovery must not replay the rejected cursor update")
				_, found, err := store.LoadUploadProgress(ctx, key, ids[0])
				require.NoError(t, err)
				require.False(t, found, "background recovery must not replay the rejected progress update")
				require.NoError(t, update(), "an explicit retry may apply the requested update after registration durability is confirmed")
				snapshot, err = store.Load(ctx, key)
				require.NoError(t, err)
				require.Equal(t, operation == "advance", snapshot.Complete(ids))
				_, found, err = store.LoadUploadProgress(ctx, key, ids[0])
				require.NoError(t, err)
				require.Equal(t, operation == "progress", found)
			})
		})
	}
}

func TestPublishedRegistrationManualSaveReportsUnconfirmedDurabilityOnClose(t *testing.T) {
	store, err := NewFilesystemStore(t.TempDir(), "source", WithSaveRetryDisabled())
	require.NoError(t, err)
	failure := errors.New("directory sync unavailable")
	setCheckpointDirectorySyncForTest(store, func(string) error { return failure })
	err = store.RegisterDestinations(context.Background(), []objectstorage.DestinationID{"primary"}, nil)
	require.ErrorIs(t, err, failure)
	require.False(t, IsPendingSaveOnly(err))
	require.ErrorIs(t, store.PendingSaveError(context.Background()), failure)
	setCheckpointDirectorySyncForTest(store, infrafsstore.SyncDirectory)
	require.ErrorIs(t, store.RegisterDestinations(context.Background(), []objectstorage.DestinationID{"primary"}, nil), failure,
		"the one-shot mode does not retry persistence on another API call")
	require.ErrorIs(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)), failure, "publishing the manifest alone does not complete its durable save")
	boundedwait.Receive(t, store.Done(), "checkpoint save worker")
	assertPublishedRegistrationIDs(t, store, []objectstorage.DestinationID{"primary"})
	reopened, err := NewFilesystemStore(store.Directory(), "source", WithSaveRetryDisabled())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, boundedwait.Close(t, "reopened.Close()", reopened.Close, boundedwait.ForceCloserOf(reopened)))
	}()
	key := CheckpointKey{Generation: "20261021-1", Compressed: true}
	require.NoError(t, reopened.RegisterDestinations(context.Background(), []objectstorage.DestinationID{"primary"}, []CheckpointKey{key}))
	snapshot, err := reopened.Load(context.Background(), key)
	require.NoError(t, err)
	require.False(t, snapshot.DestinationComplete("primary"), "manual recovery after reopening must not recompute the initial exclusion boundary")
}

func TestRegisteredDestinationsRemainUsableDuringAcceptedCursorSaveFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newCheckpointTestStore(t, "source")
		ids := []objectstorage.DestinationID{"primary"}
		require.NoError(t, store.RegisterDestinations(ctx, ids, nil))
		restore := blockCheckpointReplacement(t, store)
		key := CheckpointKey{Generation: "20261021-1", Compressed: true}
		snapshot, err := store.AdvanceCompletionCursor(ctx, key, ids)
		pending := pendingSaveForTest(t, err)
		require.True(t, snapshot.Complete(ids))
		// 登録はすでに永続化済みです。独立に受理したcursorの保存を再試行している間も、
		// uploadの継続処理は変更のない再登録(no-op refresh)を引き続き使えなければなりません。
		require.NoError(t, store.RegisterDestinations(ctx, ids, []CheckpointKey{key}))
		require.ErrorIs(t, store.PendingSaveError(ctx), pending.Err, "registration must not clear the cursor persistence failure")
		loaded, err := store.Load(ctx, key)
		require.NoError(t, err)
		require.Equal(t, snapshot, loaded)
		restore()
		advanceCheckpointRecoveryForTest()
		require.NoError(t, store.PendingSaveError(ctx))
	})
}

func TestPublishedRegistrationRecoverySyncsManifestBeforeLatestCursorAndCloseCompletes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newCheckpointTestStore(t, "source")
		defer func() {
			if store.PendingSaveError(ctx) != nil {
				_ = store.ForceClose()
			}
		}()
		first := CheckpointKey{Generation: "20261021-1", Compressed: true}
		latest := CheckpointKey{Generation: "20261021-2", Compressed: true}
		_, err := store.AdvanceCompletionCursor(ctx, first, []objectstorage.DestinationID{"primary"})
		require.NoError(t, err)
		cursorFailure := errors.New("cursor directory sync failed")
		setCheckpointDirectorySyncForTest(store, func(string) error { return cursorFailure })
		_, err = store.AdvanceCompletionCursor(ctx, latest, []objectstorage.DestinationID{"primary"})
		require.ErrorIs(t, err, cursorFailure)
		require.True(t, IsPendingSaveOnly(err), "the primary cursor update was accepted")
		registrationFailure := errors.New("registration directory sync failed")
		setCheckpointDirectorySyncForTest(store, func(string) error { return registrationFailure })
		err = store.RegisterDestinations(ctx, []objectstorage.DestinationID{"secondary"}, []CheckpointKey{latest})
		require.ErrorIs(t, err, registrationFailure)
		require.False(t, IsPendingSaveOnly(err), "the secondary initial cursor was not accepted")
		closed := make(chan error, 1)
		go func() { closed <- store.Close() }()
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("Close returned before registration durability was confirmed: %v", err)
		default:
		}
		// 公開後の実際のdirectory同期の境界をそれぞれ観測します。復旧したcursor文書は、
		// manifestを同期した後にだけ公開されなければなりません。
		var cursorSnapshots [][]byte
		var readErrors []error
		setCheckpointDirectorySyncForTest(store, func(directory string) error {
			payload, err := os.ReadFile(store.documentPath())
			cursorSnapshots = append(cursorSnapshots, payload)
			readErrors = append(readErrors, err)
			return infrafsstore.SyncDirectory(directory)
		})
		advanceCheckpointRecoveryForTest()
		require.NoError(t, boundedwait.ReceiveInBubble(t, closed, "receive from closed"))
		require.Len(t, cursorSnapshots, 2, "both manifest and cursor document must reach directory sync")
		for index, payload := range cursorSnapshots {
			require.NoError(t, readErrors[index])
			var document checkpointDocument
			require.NoError(t, json.Unmarshal(payload, &document))
			expected := []DestinationCursor{{DestinationID: "primary", Generation: latest.Generation}}
			if index == 1 {
				expected = append(expected, DestinationCursor{DestinationID: "secondary"})
			}
			require.Equal(t, expected, document.Cursors)
		}
		assertPublishedRegistrationIDs(t, store, []objectstorage.DestinationID{"primary", "secondary"})
		restarted, err := NewFilesystemStore(store.Directory(), "source", WithMissingCheckpointRecovery(false))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, boundedwait.CloseInBubble(t, "restarted.Close()", restarted.Close, restarted.ForceClose))
		}()
		snapshot, err := restarted.Load(ctx, latest)
		require.NoError(t, err)
		require.True(t, snapshot.DestinationComplete("primary"), "recovery must preserve the earlier accepted cursor update")
		require.False(t, snapshot.DestinationComplete("secondary"), "recovery must not accept the failed initial exclusion boundary")
	})
}

func assertPublishedRegistrationIDs(t *testing.T, store *FilesystemStore, expected []objectstorage.DestinationID) {
	t.Helper()
	payload, err := os.ReadFile(store.registrationsPath())
	require.NoError(t, err)
	var document destinationRegistrationDocument
	require.NoError(t, json.Unmarshal(payload, &document))
	require.Equal(t, expected, document.DestinationIDs)
}

func setCheckpointDirectorySyncForTest(store *FilesystemStore, syncDirectory func(string) error) {
	store.stateOrSaveMu.Lock()
	defer store.stateOrSaveMu.Unlock()
	store.syncDirectory = syncDirectory
}
