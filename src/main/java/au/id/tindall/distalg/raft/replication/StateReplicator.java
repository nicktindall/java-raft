package au.id.tindall.distalg.raft.replication;

public interface StateReplicator {

    enum ReplicationResult {
        SUCCESS,
        SWITCH_TO_LOG_REPLICATION,
        SWITCH_TO_SNAPSHOT_REPLICATION,
        SKIPPED
    }

    ReplicationResult sendNextReplicationMessage(boolean force);

    void logSuccessSnapshotResponse(int lastIndex, int lastOffset);
}
