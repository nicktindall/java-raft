package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

public interface StateReplicator<ID extends Serializable> {

    enum ReplicationResult {
        StayInCurrentMode,
        SwitchToLogReplication,
        SwitchToSnapshotReplication
    }

    ReplicationResult sendNextReplicationMessage();

    void logSuccessResponse(int lastAppendedIndex);

    void logSuccessSnapshotResponse(int lastIndex, int lastOffset);

    int getMatchIndex();

    int getNextIndex();

    void logFailedResponse(Integer earliestPossibleMatchIndex);
}
