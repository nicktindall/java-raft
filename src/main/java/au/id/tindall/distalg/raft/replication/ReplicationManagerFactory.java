package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.cluster.Configuration;

import java.io.Serializable;

public class ReplicationManagerFactory<ID extends Serializable> {

    private final Configuration<ID> configuration;
    private final LogReplicatorFactory<ID> logReplicatorFactory;

    public ReplicationManagerFactory(Configuration<ID> configuration, LogReplicatorFactory<ID> logReplicatorFactory) {
        this.configuration = configuration;
        this.logReplicatorFactory = logReplicatorFactory;
    }

    public ReplicationManager<ID> createReplicationManager() {
        return new ReplicationManager<>(configuration, logReplicatorFactory);
    }
}
