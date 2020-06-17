package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

import au.id.tindall.distalg.raft.comms.Cluster;

public class LogReplicatorFactory<ID extends Serializable> {

    private final int maxBatchSize;

    public LogReplicatorFactory(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public LogReplicator<ID> createLogReplicator(Cluster<ID> cluster, ID followerId, int nextIndex) {
        return new LogReplicator<>(cluster, followerId, maxBatchSize, nextIndex);
    }
}
