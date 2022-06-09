package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

public interface StateReplicator<ID extends Serializable> {

    void sendNextReplicationMessage();

    void logSuccessResponse(int lastAppendedIndex);

    int getMatchIndex();

    int getNextIndex();

    void logFailedResponse(Integer followerLastLogIndex);
}
