package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.cluster.Configuration;

import java.io.Serializable;

public class ReplicationManagerFactory<ID extends Serializable> {

    private final Configuration<ID> configuration;
    private final SingleClientReplicatorFactory<ID> replicatorFactory;

    public ReplicationManagerFactory(Configuration<ID> configuration, SingleClientReplicatorFactory<ID> replicatorFactory) {
        this.configuration = configuration;
        this.replicatorFactory = replicatorFactory;
    }

    public ReplicationManager<ID> createReplicationManager() {
        return new ReplicationManager<>(configuration, replicatorFactory);
    }
}
