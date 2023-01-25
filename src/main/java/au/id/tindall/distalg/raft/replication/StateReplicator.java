package au.id.tindall.distalg.raft.replication;

public interface StateReplicator {

    enum ReplicationResult {
        STAY_IN_CURRENT_MODE,
        SWITCH_TO_LOG_REPLICATION,
        SWITCH_TO_SNAPSHOT_REPLICATION
    }

    ReplicationResult sendNextReplicationMessage();

    void logSuccessSnapshotResponse(int lastIndex, int lastOffset);
}
