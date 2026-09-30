package system

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	storagesnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	nbv1 "github.com/noobaa/noobaa-operator/v5/pkg/apis/noobaa/v1alpha1"
	"github.com/noobaa/noobaa-operator/v5/pkg/cnpg"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CNPG label and annotation keys. The operator imports only the CNPG API package, so these are copied from
// https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.29.1/pkg/utils/labels_annotations.go
const (
	cnpgClusterLabel              = "cnpg.io/cluster"
	cnpgScheduledBackupLabel      = "cnpg.io/scheduled-backup"
	cnpgBackupNameLabel           = "cnpg.io/backupName"
	cnpgFencedInstancesAnnotation = "cnpg.io/fencedInstances"
)

const (
	// defaultDBBackupTimeout is how long a scheduled backup may stay started before it is considered stuck.
	// A volume snapshot backup normally takes seconds. Offline backups keep the target instance fenced until the
	// snapshot is provisioned, so this also bounds how long the standby can be down because of a backup.
	defaultDBBackupTimeout = 10 * time.Minute
	minDBBackupTimeout     = time.Minute

	// orphanSnapshotMinAge protects snapshots whose Backup was just created but is not in the cache yet
	orphanSnapshotMinAge = time.Minute

	backupRequeueSlack = 5 * time.Second
	minBackupRequeue   = 10 * time.Second

	// orphanCleanupRequeue is when to come back after deleting a backup, to delete its snapshot if it is not ready
	orphanCleanupRequeue = 30 * time.Second

	// backupPhaseTimedOut is reported in the NooBaa status for a backup the operator stopped after the timeout
	backupPhaseTimedOut = "timedOut"

	eventReasonDBBackupTimedOut = "DBBackupTimedOut"
	eventReasonDBBackupFailed   = "DBBackupFailed"
)

// backupSummary holds the backups of the scheduled backup that matter for the status
type backupSummary struct {
	latest          *cnpgv1.Backup
	latestCompleted *cnpgv1.Backup
	latestFailed    *cnpgv1.Backup
}

// reconcileStuckBackups stops scheduled backups that stay started longer than the timeout.
// CNPG waits forever for a volume snapshot that never reports progress or error, and an offline backup keeps
// the target instance fenced all that time. CNPG has no way to cancel a backup, so the operator deletes the
// Backup and removes the fencing it left behind. The snapshot is removed later by cleanupOrphanBackupSnapshots.
// It runs before the cluster readiness check and regardless of the backup spec, and it logs errors instead of
// returning them, so a stuck backup is always handled and never blocks the rest of the DB reconcile.
func (r *Reconciler) reconcileStuckBackups() {
	if r.CNPGCluster.UID == "" {
		return
	}

	timeout, err := getDBBackupTimeout(r.NooBaa.Annotations)
	if err != nil {
		r.cnpgLogError("invalid %s annotation, using the default timeout %s. error: %v",
			nbv1.DBBackupTimeout, defaultDBBackupTimeout, err)
	}

	backups, err := r.listScheduledBackups()
	if err != nil {
		r.cnpgLogError("got error listing scheduled backups. error: %v", err)
		return
	}

	now := time.Now()
	for _, backup := range findStuckBackups(backups, now, timeout) {
		if err := r.stopStuckBackup(&backup, now, timeout); err != nil {
			r.cnpgLogError("got error stopping stuck backup %s. error: %v", backup.Name, err)
		}
	}

	// nothing wakes the operator while a backup waits on a snapshot, so come back when it is due
	r.requeueBackupReconcile(getBackupRequeueAfter(backups, now, timeout))
}

// requeueBackupReconcile asks for a reconcile after the given time, keeping the earliest request. 0 is ignored.
func (r *Reconciler) requeueBackupReconcile(after time.Duration) {
	if after > 0 && (r.backupRequeueAfter == 0 || after < r.backupRequeueAfter) {
		r.backupRequeueAfter = after
	}
}

// markBackupDeleted remembers a backup deleted in this reconcile and requeues to clean up its snapshot
func (r *Reconciler) markBackupDeleted(backupName string) {
	if r.deletedBackups == nil {
		r.deletedBackups = map[string]bool{}
	}
	r.deletedBackups[backupName] = true
	r.requeueBackupReconcile(orphanCleanupRequeue)
}

// stopStuckBackup deletes a stuck backup, unfences its target instance and reports the timeout.
// The Backup is deleted before anything else so CNPG stops working on it.
// The VolumeSnapshot is kept on purpose: a CNPG reconcile that is already in flight and finds no snapshot
// would fence the instance again and take a new snapshot.
func (r *Reconciler) stopStuckBackup(backup *cnpgv1.Backup, now time.Time, timeout time.Duration) error {
	targetPod := ""
	if backup.Status.InstanceID != nil {
		targetPod = backup.Status.InstanceID.PodName
	}
	r.cnpgLog("backup %s on pod %q started at %s and did not complete within %s, stopping it",
		backup.Name, targetPod, backup.Status.StartedAt, timeout)

	if err := r.Client.Delete(r.Ctx, cnpg.GetCnpgBackupObj(backup.Namespace, backup.Name)); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete backup: %w", err)
	}
	r.markBackupDeleted(backup.Name)

	if err := r.unfenceBackupTarget(targetPod); err != nil {
		return fmt.Errorf("failed to unfence pod %q: %w", targetPod, err)
	}

	msg := fmt.Sprintf("backup %s on pod %q did not complete within %s and was stopped by the operator",
		backup.Name, targetPod, timeout)
	if backupStatus := r.NooBaa.Status.DBStatus.BackupStatus; backupStatus != nil {
		recordBackupTimeout(backupStatus, backup.Name, now, msg)
	}
	r.recordWarningEvent(eventReasonDBBackupTimedOut, msg)
	return nil
}

// unfenceBackupTarget removes the fencing of the backup target pod, only if it is the only fenced instance.
// Any other fencing was not done by this backup (e.g. manual fencing) and is left as is.
func (r *Reconciler) unfenceBackupTarget(targetPod string) error {
	fencedOnlyByTarget, err := isFencedOnlyBy(r.CNPGCluster.Annotations, targetPod)
	if err != nil {
		return err
	}
	if !fencedOnlyByTarget {
		r.cnpgLog("fenced instances are %q, not only the backup target %q. leaving fencing as is",
			r.CNPGCluster.Annotations[cnpgFencedInstancesAnnotation], targetPod)
		return nil
	}

	r.cnpgLog("unfencing pod %q", targetPod)
	patch := client.MergeFrom(r.CNPGCluster.DeepCopy())
	delete(r.CNPGCluster.Annotations, cnpgFencedInstancesAnnotation)
	return r.Client.Patch(r.Ctx, r.CNPGCluster, patch)
}

// cleanupOrphanBackupSnapshots deletes snapshots of scheduled backups that no longer exist and never became ready.
// These are left by stopStuckBackup, by pruned failed backups, and by CNPG itself when a backup fails.
// Ready snapshots are valid backups and are managed only by the retention.
func (r *Reconciler) cleanupOrphanBackupSnapshots() {
	if r.CNPGCluster.UID == "" {
		return
	}

	backups, err := r.listScheduledBackups()
	if err != nil {
		r.cnpgLogError("got error listing scheduled backups. error: %v", err)
		return
	}

	snapshots := storagesnapshotv1.VolumeSnapshotList{}
	if err := r.Client.List(r.Ctx, &snapshots,
		client.InNamespace(r.CNPGCluster.Namespace),
		client.MatchingLabels{cnpgClusterLabel: r.CNPGCluster.Name},
		client.HasLabels{cnpgBackupNameLabel},
	); err != nil {
		r.cnpgLogError("got error listing volume snapshots. error: %v", err)
		return
	}

	// backups deleted in this reconcile still count as existing: a CNPG reconcile already in flight may hold
	// such a backup, and if it finds no snapshot it fences the instance again. Their snapshots are deleted
	// in a later reconcile (see markBackupDeleted).
	existingBackups := map[string]bool{}
	for _, backup := range backups {
		existingBackups[backup.Name] = true
	}
	for name := range r.deletedBackups {
		existingBackups[name] = true
	}

	for _, snapshot := range findOrphanSnapshots(snapshots.Items, existingBackups, r.getBackupResourceName(), time.Now()) {
		r.cnpgLog("deleting snapshot %s of backup %s: the backup no longer exists and the snapshot is not ready",
			snapshot.Name, snapshot.Labels[cnpgBackupNameLabel])
		if err := r.Client.Delete(r.Ctx, &snapshot); err != nil && !errors.IsNotFound(err) {
			r.cnpgLogError("got error deleting snapshot %s. error: %v", snapshot.Name, err)
		}
	}
}

// reconcileBackupStatus reports the latest scheduled backups in the NooBaa status,
// and emits an event once for every new failed backup.
func (r *Reconciler) reconcileBackupStatus() error {
	backups, err := r.listScheduledBackups()
	if err != nil {
		return err
	}
	backupStatus := r.NooBaa.Status.DBStatus.BackupStatus
	if failed := mergeBackupStatus(backupStatus, summarizeBackups(backups)); failed != nil {
		r.recordWarningEvent(eventReasonDBBackupFailed,
			fmt.Sprintf("backup %s failed: %s", failed.Name, failed.Status.Error))
	}
	return nil
}

// listScheduledBackups lists the backups created by the scheduled backup.
// Backups deleted during this reconcile are skipped, since the cached client may still return them.
func (r *Reconciler) listScheduledBackups() ([]cnpgv1.Backup, error) {
	backupList := cnpg.GetCnpgBackupListObj(r.CNPGCluster.Namespace)
	if err := r.Client.List(r.Ctx, backupList,
		client.InNamespace(r.CNPGCluster.Namespace),
		client.MatchingLabels{cnpgScheduledBackupLabel: r.getBackupResourceName()},
	); err != nil {
		return nil, err
	}
	backups := []cnpgv1.Backup{}
	for _, backup := range backupList.Items {
		if !r.deletedBackups[backup.Name] {
			backups = append(backups, backup)
		}
	}
	return backups, nil
}

func (r *Reconciler) recordWarningEvent(reason string, msg string) {
	if r.Recorder != nil {
		r.Recorder.Eventf(r.NooBaa, nil, corev1.EventTypeWarning, reason, reason, msg)
	}
}

// getDBBackupTimeout returns the backup timeout, overridden by the NooBaa annotation.
// On an invalid value it returns the default timeout together with the error.
func getDBBackupTimeout(annotations map[string]string) (time.Duration, error) {
	value, ok := annotations[nbv1.DBBackupTimeout]
	if !ok {
		return defaultDBBackupTimeout, nil
	}
	timeout, err := time.ParseDuration(value)
	if err != nil {
		return defaultDBBackupTimeout, err
	}
	if timeout < minDBBackupTimeout {
		return defaultDBBackupTimeout, fmt.Errorf("timeout %s is shorter than the minimum %s", timeout, minDBBackupTimeout)
	}
	return timeout, nil
}

// isBackupStarted returns true for a backup that may hold resources: the target instance may be fenced
// (offline backup) or in backup mode (online backup). A pending backup did not start yet, and a finalizing
// backup has already released them.
func isBackupStarted(backup *cnpgv1.Backup) bool {
	return backup.Status.StartedAt != nil &&
		(backup.Status.Phase == cnpgv1.BackupPhaseStarted || backup.Status.Phase == cnpgv1.BackupPhaseRunning)
}

// findStuckBackups returns the started backups that did not complete within the timeout
func findStuckBackups(backups []cnpgv1.Backup, now time.Time, timeout time.Duration) []cnpgv1.Backup {
	stuck := []cnpgv1.Backup{}
	for _, backup := range backups {
		if isBackupStarted(&backup) && now.Sub(backup.Status.StartedAt.Time) > timeout {
			stuck = append(stuck, backup)
		}
	}
	return stuck
}

// getBackupRequeueAfter returns when the earliest started backup reaches its timeout, or 0 if none is started.
// Stuck backups are skipped since they are stopped in the current reconcile.
func getBackupRequeueAfter(backups []cnpgv1.Backup, now time.Time, timeout time.Duration) time.Duration {
	var requeueAfter time.Duration
	for _, backup := range backups {
		if !isBackupStarted(&backup) {
			continue
		}
		remaining := timeout - now.Sub(backup.Status.StartedAt.Time)
		if remaining < 0 {
			continue
		}
		remaining = max(remaining+backupRequeueSlack, minBackupRequeue)
		if requeueAfter == 0 || remaining < requeueAfter {
			requeueAfter = remaining
		}
	}
	return requeueAfter
}

// isFencedOnlyBy returns true if the fenced instances annotation lists exactly the given pod
func isFencedOnlyBy(annotations map[string]string, pod string) (bool, error) {
	value := annotations[cnpgFencedInstancesAnnotation]
	if value == "" || pod == "" {
		return false, nil
	}
	fenced := []string{}
	if err := json.Unmarshal([]byte(value), &fenced); err != nil {
		return false, fmt.Errorf("failed to parse %s annotation %q: %w", cnpgFencedInstancesAnnotation, value, err)
	}
	return len(fenced) == 1 && fenced[0] == pod, nil
}

// findOrphanSnapshots returns the snapshots of scheduled backups that are not in existingBackups
// and are not ready to use
func findOrphanSnapshots(
	snapshots []storagesnapshotv1.VolumeSnapshot,
	existingBackups map[string]bool,
	scheduledBackupName string,
	now time.Time,
) []storagesnapshotv1.VolumeSnapshot {
	orphans := []storagesnapshotv1.VolumeSnapshot{}
	for _, snapshot := range snapshots {
		backupName := snapshot.Labels[cnpgBackupNameLabel]
		if !strings.HasPrefix(backupName, scheduledBackupName) ||
			isSnapshotReady(&snapshot) ||
			now.Sub(snapshot.CreationTimestamp.Time) < orphanSnapshotMinAge ||
			existingBackups[backupName] {
			continue
		}
		orphans = append(orphans, snapshot)
	}
	return orphans
}

func isSnapshotReady(snapshot *storagesnapshotv1.VolumeSnapshot) bool {
	return snapshot.Status != nil && snapshot.Status.ReadyToUse != nil && *snapshot.Status.ReadyToUse
}

// isNewerBackup returns true if a was created after b. Scheduled backup names end with a timestamp,
// so the name breaks ties between backups created in the same second.
func isNewerBackup(a *cnpgv1.Backup, b *cnpgv1.Backup) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return b.CreationTimestamp.Before(&a.CreationTimestamp)
	}
	return a.Name > b.Name
}

// summarizeBackups finds the latest backup, the latest completed backup and the latest failed backup
func summarizeBackups(backups []cnpgv1.Backup) backupSummary {
	summary := backupSummary{}
	for i := range backups {
		backup := &backups[i]
		if summary.latest == nil || isNewerBackup(backup, summary.latest) {
			summary.latest = backup
		}
		switch backup.Status.Phase {
		case cnpgv1.BackupPhaseCompleted:
			if summary.latestCompleted == nil || isNewerBackup(backup, summary.latestCompleted) {
				summary.latestCompleted = backup
			}
		case cnpgv1.BackupPhaseFailed:
			if summary.latestFailed == nil || isNewerBackup(backup, summary.latestFailed) {
				summary.latestFailed = backup
			}
		}
	}
	return summary
}

// backupFailedAt returns when CNPG flagged the backup as failed
func backupFailedAt(backup *cnpgv1.Backup) metav1.Time {
	if backup.Status.ReconciliationTerminatedAt != nil {
		return *backup.Status.ReconciliationTerminatedAt
	}
	return backup.CreationTimestamp
}

// recordBackupTimeout reports in the status a backup stopped after the timeout.
// The backup is deleted, so mergeBackupStatus keeps this record until a newer backup shows up.
func recordBackupTimeout(status *nbv1.DBBackupStatus, backupName string, now time.Time, msg string) {
	stoppedAt := metav1.NewTime(now)
	status.LastBackupName = backupName
	status.LastBackupPhase = backupPhaseTimedOut
	status.LastFailedBackupName = backupName
	status.LastFailedBackupTime = &stoppedAt
	status.LastBackupError = msg
}

// mergeBackupStatus updates the backup status from the existing backups.
// Backups that timed out were deleted, so their record in the status is replaced only by newer backups.
// It returns the failed backup that is reported for the first time, or nil.
func mergeBackupStatus(status *nbv1.DBBackupStatus, summary backupSummary) *cnpgv1.Backup {
	if completed := summary.latestCompleted; completed != nil {
		completedAt := completed.CreationTimestamp
		if completed.Status.StoppedAt != nil {
			completedAt = *completed.Status.StoppedAt
		}
		status.LastBackupTime = &completedAt
	}

	// evaluated before the failure fields below are updated, since it compares against the recorded timeout
	if latest := summary.latest; latest != nil {
		// keep a timeout record while the latest existing backup is an older, finished one
		keepTimeout := status.LastBackupPhase == backupPhaseTimedOut &&
			status.LastFailedBackupTime != nil &&
			latest.CreationTimestamp.Before(status.LastFailedBackupTime) &&
			(latest.Status.Phase == cnpgv1.BackupPhaseCompleted || latest.Status.Phase == cnpgv1.BackupPhaseFailed)
		if !keepTimeout {
			status.LastBackupName = latest.Name
			status.LastBackupPhase = string(latest.Status.Phase)
		}
	}

	var newFailure *cnpgv1.Backup
	if failed := summary.latestFailed; failed != nil && failed.Name != status.LastFailedBackupName {
		failedAt := backupFailedAt(failed)
		if status.LastFailedBackupTime == nil || status.LastFailedBackupTime.Before(&failedAt) {
			status.LastFailedBackupName = failed.Name
			status.LastFailedBackupTime = &failedAt
			status.LastBackupError = failed.Status.Error
			newFailure = failed
		}
	}

	return newFailure
}
