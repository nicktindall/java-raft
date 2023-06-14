package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Serializable;

public class LogReplicatorFactory<I extends Serializable> {

    private final Log log;
    private final PersistentState<I> persistentState;
    private final Cluster<I> cluster;
    private final int maxBatchSize;

    public LogReplicatorFactory(Log log, PersistentState<I> persistentState, Cluster<I> cluster,
                                int maxBatchSize) {
        this.log = log;
        this.cluster = cluster;
        this.persistentState = persistentState;
        this.maxBatchSize = maxBatchSize;
    }

    public LogReplicator<I> createLogReplicator(ReplicationState<I> replicationState) {
        return new LogReplicator<>(log, persistentState.getCurrentTerm(), cluster, maxBatchSize, replicationState);
    }
}
