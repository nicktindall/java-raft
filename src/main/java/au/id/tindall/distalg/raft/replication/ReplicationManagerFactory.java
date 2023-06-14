package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.cluster.Configuration;

import java.io.Serializable;

public class ReplicationManagerFactory<I extends Serializable> {

    private final Configuration<I> configuration;
    private final SingleClientReplicatorFactory<I> replicatorFactory;

    public ReplicationManagerFactory(Configuration<I> configuration, SingleClientReplicatorFactory<I> replicatorFactory) {
        this.configuration = configuration;
        this.replicatorFactory = replicatorFactory;
    }

    public ReplicationManager<I> createReplicationManager() {
        return new ReplicationManager<>(configuration, replicatorFactory);
    }
}
