package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.comms.Cluster;

import java.io.Serializable;

public class ReplicationManagerFactory<ID extends Serializable> {

    private final Cluster<ID> cluster;
    private final LogReplicatorFactory<ID> logReplicatorFactory;

    public ReplicationManagerFactory(Cluster<ID> cluster, LogReplicatorFactory<ID> logReplicatorFactory) {
        this.cluster = cluster;
        this.logReplicatorFactory = logReplicatorFactory;
    }

    public ReplicationManager<ID> createReplicationManager() {
        return new ReplicationManager<>(cluster, logReplicatorFactory);
    }
}
