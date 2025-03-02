package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.cluster.Configuration;

public class ReplicationManagerFactory<I> {

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
