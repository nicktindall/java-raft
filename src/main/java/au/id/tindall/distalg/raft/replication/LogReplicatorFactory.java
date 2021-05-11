package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Serializable;

public class LogReplicatorFactory<ID extends Serializable> {

    private final Log log;
    private final PersistentState<ID> persistentState;
    private final Cluster<ID> cluster;
    private final int maxBatchSize;
    private final ReplicationSchedulerFactory replicationSchedulerFactory;

    public LogReplicatorFactory(Log log, PersistentState<ID> persistentState, Cluster<ID> cluster,
                                int maxBatchSize, ReplicationSchedulerFactory replicationSchedulerFactory) {
        this.log = log;
        this.cluster = cluster;
        this.persistentState = persistentState;
        this.maxBatchSize = maxBatchSize;
        this.replicationSchedulerFactory = replicationSchedulerFactory;
    }

    public LogReplicator<ID> createLogReplicator(ID followerId) {
        return new LogReplicator<>(log, persistentState.getCurrentTerm(), cluster, followerId, maxBatchSize, log.getNextLogIndex(), replicationSchedulerFactory.create());
    }
}
