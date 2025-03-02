package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.log.Log;

public class ReplicationStateFactory<I> {

    private final Log log;

    public ReplicationStateFactory(Log log) {
        this.log = log;
    }

    public ReplicationState<I> createReplicationState(I folllowerId, MatchIndexAdvancedListener<I> matchIndexAdvancedListener) {
        return new ReplicationState<>(folllowerId, log.getNextLogIndex(), matchIndexAdvancedListener);
    }
}
