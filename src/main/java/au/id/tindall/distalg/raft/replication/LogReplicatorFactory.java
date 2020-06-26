package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;

public class LogReplicatorFactory<ID extends Serializable> {

    private final int maxBatchSize;
    private final ReplicationSchedulerFactory replicationSchedulerFactory;

    public LogReplicatorFactory(int maxBatchSize, ReplicationSchedulerFactory replicationSchedulerFactory) {
        this.maxBatchSize = maxBatchSize;
        this.replicationSchedulerFactory = replicationSchedulerFactory;
    }

    public LogReplicator<ID> createLogReplicator(Log log, Term term, Cluster<ID> cluster, ID followerId, int nextIndex) {
        return new LogReplicator<>(log, term, cluster, followerId, maxBatchSize, nextIndex, replicationSchedulerFactory.create());
    }
}
