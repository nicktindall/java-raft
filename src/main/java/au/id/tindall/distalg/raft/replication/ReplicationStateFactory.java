package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.log.Log;

import java.io.Serializable;

public class ReplicationStateFactory<ID extends Serializable> {

    private final Log log;

    public ReplicationStateFactory(Log log) {
        this.log = log;
    }

    public ReplicationState<ID> createReplicationState(ID folllowerId) {
        return new ReplicationState<>(folllowerId, log.getNextLogIndex());
    }
}
