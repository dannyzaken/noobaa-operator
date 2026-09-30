package system

import (
	"testing"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	storagesnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	nbv1 "github.com/noobaa/noobaa-operator/v5/pkg/apis/noobaa/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var backupTestNow = time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)

// testBackup builds a backup created createdAgo before backupTestNow, started startedAgo before it (if >= 0)
func testBackup(name string, phase string, createdAgo time.Duration, startedAgo time.Duration) cnpgv1.Backup {
	backup := cnpgv1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(backupTestNow.Add(-createdAgo)),
		},
		Status: cnpgv1.BackupStatus{Phase: cnpgv1.BackupPhase(phase)},
	}
	if startedAgo >= 0 {
		startedAt := metav1.NewTime(backupTestNow.Add(-startedAgo))
		backup.Status.StartedAt = &startedAt
	}
	return backup
}

func backupNames(backups []cnpgv1.Backup) []string {
	names := []string{}
	for _, b := range backups {
		names = append(names, b.Name)
	}
	return names
}

func TestGetDBBackupTimeout(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		expected    time.Duration
		expectErr   bool
	}{
		{"nil annotations", nil, defaultDBBackupTimeout, false},
		{"unset", map[string]string{"other": "x"}, defaultDBBackupTimeout, false},
		{"valid", map[string]string{nbv1.DBBackupTimeout: "20m"}, 20 * time.Minute, false},
		{"minimum", map[string]string{nbv1.DBBackupTimeout: "1m"}, time.Minute, false},
		{"below minimum", map[string]string{nbv1.DBBackupTimeout: "30s"}, defaultDBBackupTimeout, true},
		{"invalid", map[string]string{nbv1.DBBackupTimeout: "ten minutes"}, defaultDBBackupTimeout, true},
		{"empty", map[string]string{nbv1.DBBackupTimeout: ""}, defaultDBBackupTimeout, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getDBBackupTimeout(tt.annotations)
			if got != tt.expected {
				t.Errorf("getDBBackupTimeout() = %s, want %s", got, tt.expected)
			}
			if (err != nil) != tt.expectErr {
				t.Errorf("getDBBackupTimeout() error = %v, expectErr %v", err, tt.expectErr)
			}
		})
	}
}

func TestFindStuckBackups(t *testing.T) {
	timeout := 10 * time.Minute
	backups := []cnpgv1.Backup{
		testBackup("started-stuck", cnpgv1.BackupPhaseStarted, 11*time.Minute, 11*time.Minute),
		testBackup("running-stuck", cnpgv1.BackupPhaseRunning, 30*time.Minute, 30*time.Minute),
		testBackup("started-in-time", cnpgv1.BackupPhaseStarted, 5*time.Minute, 5*time.Minute),
		testBackup("started-at-boundary", cnpgv1.BackupPhaseStarted, timeout, timeout),
		testBackup("started-no-start-time", cnpgv1.BackupPhaseStarted, time.Hour, -1),
		testBackup("pending-old", cnpgv1.BackupPhasePending, time.Hour, -1),
		testBackup("finalizing-old", cnpgv1.BackupPhaseFinalizing, time.Hour, time.Hour),
		testBackup("completed-old", cnpgv1.BackupPhaseCompleted, time.Hour, time.Hour),
		testBackup("failed-old", cnpgv1.BackupPhaseFailed, time.Hour, time.Hour),
	}
	got := backupNames(findStuckBackups(backups, backupTestNow, timeout))
	expected := []string{"started-stuck", "running-stuck"}
	if len(got) != len(expected) || got[0] != expected[0] || got[1] != expected[1] {
		t.Errorf("findStuckBackups() = %v, want %v", got, expected)
	}
}

func TestGetBackupRequeueAfter(t *testing.T) {
	timeout := 10 * time.Minute
	tests := []struct {
		name     string
		backups  []cnpgv1.Backup
		expected time.Duration
	}{
		{"no backups", nil, 0},
		{"only finished and pending", []cnpgv1.Backup{
			testBackup("a", cnpgv1.BackupPhaseCompleted, time.Hour, time.Hour),
			testBackup("b", cnpgv1.BackupPhasePending, time.Minute, -1),
			testBackup("c", cnpgv1.BackupPhaseFinalizing, time.Minute, time.Minute),
		}, 0},
		{"started 4m ago", []cnpgv1.Backup{
			testBackup("a", cnpgv1.BackupPhaseStarted, 4*time.Minute, 4*time.Minute),
		}, 6*time.Minute + backupRequeueSlack},
		{"earliest deadline wins", []cnpgv1.Backup{
			testBackup("a", cnpgv1.BackupPhaseStarted, 2*time.Minute, 2*time.Minute),
			testBackup("b", cnpgv1.BackupPhaseRunning, 7*time.Minute, 7*time.Minute),
		}, 3*time.Minute + backupRequeueSlack},
		{"close to deadline uses minimum", []cnpgv1.Backup{
			testBackup("a", cnpgv1.BackupPhaseStarted, timeout-time.Second, timeout-time.Second),
		}, minBackupRequeue},
		{"already stuck is skipped", []cnpgv1.Backup{
			testBackup("a", cnpgv1.BackupPhaseStarted, time.Hour, time.Hour),
		}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getBackupRequeueAfter(tt.backups, backupTestNow, timeout); got != tt.expected {
				t.Errorf("getBackupRequeueAfter() = %s, want %s", got, tt.expected)
			}
		})
	}
}

func TestRequeueBackupReconcile(t *testing.T) {
	r := &Reconciler{}
	r.requeueBackupReconcile(0)
	if r.backupRequeueAfter != 0 {
		t.Fatalf("backupRequeueAfter = %s after 0, want 0", r.backupRequeueAfter)
	}
	r.requeueBackupReconcile(5 * time.Minute)
	r.requeueBackupReconcile(10 * time.Minute)
	if r.backupRequeueAfter != 5*time.Minute {
		t.Fatalf("backupRequeueAfter = %s, want the earliest 5m", r.backupRequeueAfter)
	}
	r.markBackupDeleted("b-1")
	if r.backupRequeueAfter != orphanCleanupRequeue || !r.deletedBackups["b-1"] {
		t.Fatalf("after markBackupDeleted: requeue %s deleted %v, want %s and b-1",
			r.backupRequeueAfter, r.deletedBackups, orphanCleanupRequeue)
	}
}

func TestIsFencedOnlyBy(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		pod         string
		expected    bool
		expectErr   bool
	}{
		{"no annotations", nil, "db-2", false, false},
		{"not fenced", map[string]string{"other": "x"}, "db-2", false, false},
		{"fenced by target", map[string]string{cnpgFencedInstancesAnnotation: `["db-2"]`}, "db-2", true, false},
		{"fenced other pod", map[string]string{cnpgFencedInstancesAnnotation: `["db-1"]`}, "db-2", false, false},
		{"fenced several pods", map[string]string{cnpgFencedInstancesAnnotation: `["db-1","db-2"]`}, "db-2", false, false},
		{"fenced all instances", map[string]string{cnpgFencedInstancesAnnotation: `["*"]`}, "db-2", false, false},
		{"empty list", map[string]string{cnpgFencedInstancesAnnotation: `[]`}, "db-2", false, false},
		{"unknown target pod", map[string]string{cnpgFencedInstancesAnnotation: `["db-2"]`}, "", false, false},
		{"bad json", map[string]string{cnpgFencedInstancesAnnotation: `db-2`}, "db-2", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := isFencedOnlyBy(tt.annotations, tt.pod)
			if got != tt.expected {
				t.Errorf("isFencedOnlyBy() = %v, want %v", got, tt.expected)
			}
			if (err != nil) != tt.expectErr {
				t.Errorf("isFencedOnlyBy() error = %v, expectErr %v", err, tt.expectErr)
			}
		})
	}
}

func TestFindOrphanSnapshots(t *testing.T) {
	ready := true
	notReady := false
	snapshot := func(name string, backupName string, age time.Duration, readyToUse *bool) storagesnapshotv1.VolumeSnapshot {
		vs := storagesnapshotv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				Labels:            map[string]string{cnpgBackupNameLabel: backupName},
				CreationTimestamp: metav1.NewTime(backupTestNow.Add(-age)),
			},
		}
		if readyToUse != nil {
			vs.Status = &storagesnapshotv1.VolumeSnapshotStatus{ReadyToUse: readyToUse}
		}
		return vs
	}
	scheduled := "db-scheduled-backup"
	// includes a backup deleted in the current reconcile, which must still count as existing
	existing := map[string]bool{scheduled + "-exists": true, scheduled + "-deleted-now": true}
	snapshots := []storagesnapshotv1.VolumeSnapshot{
		snapshot("orphan-no-status", scheduled+"-gone1", time.Hour, nil),
		snapshot("orphan-not-ready", scheduled+"-gone2", time.Hour, &notReady),
		snapshot("orphan-ready", scheduled+"-gone3", time.Hour, &ready),
		snapshot("backup-exists", scheduled+"-exists", time.Hour, nil),
		snapshot("backup-deleted-now", scheduled+"-deleted-now", time.Hour, nil),
		snapshot("too-new", scheduled+"-gone4", 10*time.Second, nil),
		snapshot("manual-backup", "manual-backup", time.Hour, nil),
	}
	got := findOrphanSnapshots(snapshots, existing, scheduled, backupTestNow)
	if len(got) != 2 || got[0].Name != "orphan-no-status" || got[1].Name != "orphan-not-ready" {
		names := []string{}
		for _, s := range got {
			names = append(names, s.Name)
		}
		t.Errorf("findOrphanSnapshots() = %v, want [orphan-no-status orphan-not-ready]", names)
	}
}

func TestSummarizeBackups(t *testing.T) {
	backups := []cnpgv1.Backup{
		testBackup("b-1", cnpgv1.BackupPhaseCompleted, 4*time.Hour, 4*time.Hour),
		testBackup("b-2", cnpgv1.BackupPhaseFailed, 3*time.Hour, 3*time.Hour),
		testBackup("b-3", cnpgv1.BackupPhaseCompleted, 2*time.Hour, 2*time.Hour),
		testBackup("b-4", cnpgv1.BackupPhaseFailed, time.Hour, time.Hour),
		testBackup("b-5", cnpgv1.BackupPhaseStarted, time.Minute, time.Minute),
		// same creation second as b-5, newer by name
		testBackup("b-6", cnpgv1.BackupPhasePending, time.Minute, -1),
	}
	summary := summarizeBackups(backups)
	if summary.latest == nil || summary.latest.Name != "b-6" {
		t.Errorf("latest = %v, want b-6", summary.latest)
	}
	if summary.latestCompleted == nil || summary.latestCompleted.Name != "b-3" {
		t.Errorf("latestCompleted = %v, want b-3", summary.latestCompleted)
	}
	if summary.latestFailed == nil || summary.latestFailed.Name != "b-4" {
		t.Errorf("latestFailed = %v, want b-4", summary.latestFailed)
	}

	empty := summarizeBackups(nil)
	if empty.latest != nil || empty.latestCompleted != nil || empty.latestFailed != nil {
		t.Errorf("summarizeBackups(nil) = %+v, want empty", empty)
	}
}

func TestMergeBackupStatus(t *testing.T) {
	failedBackup := func(name string, createdAgo time.Duration, failedAgo time.Duration) cnpgv1.Backup {
		b := testBackup(name, cnpgv1.BackupPhaseFailed, createdAgo, createdAgo)
		failedAt := metav1.NewTime(backupTestNow.Add(-failedAgo))
		b.Status.ReconciliationTerminatedAt = &failedAt
		b.Status.Error = "snapshot error of " + name
		return b
	}
	completedBackup := func(name string, createdAgo time.Duration) cnpgv1.Backup {
		b := testBackup(name, cnpgv1.BackupPhaseCompleted, createdAgo, createdAgo)
		stoppedAt := metav1.NewTime(backupTestNow.Add(-createdAgo + time.Minute))
		b.Status.StoppedAt = &stoppedAt
		return b
	}
	timedOutStatus := func(timedOutAgo time.Duration) *nbv1.DBBackupStatus {
		status := &nbv1.DBBackupStatus{}
		recordBackupTimeout(status, "timed-out", backupTestNow.Add(-timedOutAgo), "timed out")
		return status
	}

	t.Run("completed and failed backups are reported", func(t *testing.T) {
		status := &nbv1.DBBackupStatus{}
		completed := completedBackup("b-1", 2*time.Hour)
		failed := failedBackup("b-2", time.Hour, 50*time.Minute)
		newFailure := mergeBackupStatus(status, summarizeBackups([]cnpgv1.Backup{completed, failed}))
		if newFailure == nil || newFailure.Name != "b-2" {
			t.Fatalf("newFailure = %v, want b-2", newFailure)
		}
		if !status.LastBackupTime.Equal(completed.Status.StoppedAt) {
			t.Errorf("LastBackupTime = %v, want %v", status.LastBackupTime, completed.Status.StoppedAt)
		}
		if status.LastBackupName != "b-2" || status.LastBackupPhase != cnpgv1.BackupPhaseFailed {
			t.Errorf("last backup = %s/%s, want b-2/failed", status.LastBackupName, status.LastBackupPhase)
		}
		if status.LastFailedBackupName != "b-2" || status.LastBackupError != "snapshot error of b-2" {
			t.Errorf("last failure = %s/%q, want b-2", status.LastFailedBackupName, status.LastBackupError)
		}

		// the same failure is reported once
		if again := mergeBackupStatus(status, summarizeBackups([]cnpgv1.Backup{completed, failed})); again != nil {
			t.Errorf("second merge reported %s again", again.Name)
		}
	})

	t.Run("older failure does not replace a recorded timeout", func(t *testing.T) {
		status := timedOutStatus(10 * time.Minute)
		old := failedBackup("b-old", 3*time.Hour, 3*time.Hour)
		if newFailure := mergeBackupStatus(status, summarizeBackups([]cnpgv1.Backup{old})); newFailure != nil {
			t.Errorf("newFailure = %s, want nil", newFailure.Name)
		}
		if status.LastFailedBackupName != "timed-out" || status.LastBackupName != "timed-out" ||
			status.LastBackupPhase != backupPhaseTimedOut {
			t.Errorf("status = %+v, want the timeout record kept", status)
		}
	})

	t.Run("older completed backup keeps the timeout as last backup", func(t *testing.T) {
		status := timedOutStatus(10 * time.Minute)
		completed := completedBackup("b-old", 3*time.Hour)
		mergeBackupStatus(status, summarizeBackups([]cnpgv1.Backup{completed}))
		if status.LastBackupName != "timed-out" || status.LastBackupPhase != backupPhaseTimedOut {
			t.Errorf("last backup = %s/%s, want timed-out", status.LastBackupName, status.LastBackupPhase)
		}
		if !status.LastBackupTime.Equal(completed.Status.StoppedAt) {
			t.Errorf("LastBackupTime = %v, want %v", status.LastBackupTime, completed.Status.StoppedAt)
		}
	})

	t.Run("pending backup created before the timeout replaces it once running", func(t *testing.T) {
		status := timedOutStatus(time.Minute)
		running := testBackup("b-next", cnpgv1.BackupPhaseStarted, 5*time.Minute, 30*time.Second)
		mergeBackupStatus(status, summarizeBackups([]cnpgv1.Backup{running}))
		if status.LastBackupName != "b-next" || status.LastBackupPhase != cnpgv1.BackupPhaseStarted {
			t.Errorf("last backup = %s/%s, want b-next/started", status.LastBackupName, status.LastBackupPhase)
		}
		if status.LastFailedBackupName != "timed-out" {
			t.Errorf("LastFailedBackupName = %s, want timed-out", status.LastFailedBackupName)
		}
	})

	t.Run("newer failure replaces a recorded timeout", func(t *testing.T) {
		status := timedOutStatus(time.Hour)
		newer := failedBackup("b-new", 30*time.Minute, 20*time.Minute)
		newFailure := mergeBackupStatus(status, summarizeBackups([]cnpgv1.Backup{newer}))
		if newFailure == nil || newFailure.Name != "b-new" {
			t.Fatalf("newFailure = %v, want b-new", newFailure)
		}
		if status.LastBackupName != "b-new" || status.LastFailedBackupName != "b-new" {
			t.Errorf("status = %+v, want b-new as last and last failed backup", status)
		}
	})
}
