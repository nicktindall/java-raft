package au.id.tindall.distalg.raft.replication;

public interface StateReplicator {

    enum ReplicationResult {
        StayInCurrentMode,
        SwitchToLogReplication,
        SwitchToSnapshotReplication
    }

    ReplicationResult sendNextReplicationMessage();

    void logSuccessSnapshotResponse(int lastIndex, int lastOffset);
}
