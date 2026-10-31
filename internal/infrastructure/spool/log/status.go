package logspool

import (
	"github.com/go-jappe/heterolog/internal/application/status"
)

// ReplayStatusOfは、supervisorのSnapshotを、statusの報告の送り直しの状態にします。nilの
// supervisorはstoppedです。
func ReplayStatusOf(supervisor *RunnerSupervisor) status.Replay {
	snapshot := supervisor.Snapshot()
	state := snapshot.State
	if state == "" {
		// まだ動き始めていないsupervisorです。
		state = RunnerSupervisorStateIdle
	}
	replay := status.Replay{
		State:        string(state),
		StartedAt:    snapshot.StartedAt,
		LastRunAt:    snapshot.LastRunReportedAt,
		LastPurgeAt:  snapshot.LastPurgeAt,
		RestartCount: snapshot.RestartCount,
		LastError:    snapshot.LastRunError,
		LastRun: status.ReplayRun{
			Records:    snapshot.LastRunReport.ProcessReport.RecordCount,
			Success:    snapshot.LastRunReport.ProcessReport.SuccessRecordCount,
			Dead:       snapshot.LastRunReport.ProcessReport.DeadRecordCount,
			Quarantine: snapshot.LastRunReport.ProcessReport.QuarantineRecordCount,
			Retryable:  snapshot.LastRunReport.ProcessReport.RetryRecordCount,
		},
	}
	// 止まったrunnerでは、止まった理由を優先して示します。
	if snapshot.State == RunnerSupervisorStateStopped && snapshot.LastRunnerStopError != "" {
		replay.LastError = snapshot.LastRunnerStopError
	}
	return replay
}
