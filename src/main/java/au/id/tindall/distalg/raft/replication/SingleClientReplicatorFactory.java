package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

public class SingleClientReplicatorFactory<ID extends Serializable> {

    private final ReplicationSchedulerFactory replicationSchedulerFactory;
    private final LogReplicatorFactory<ID> logReplicatorFactory;

    public SingleClientReplicatorFactory(ReplicationSchedulerFactory replicationSchedulerFactory, LogReplicatorFactory<ID> logReplicatorFactory) {
        this.replicationSchedulerFactory = replicationSchedulerFactory;
        this.logReplicatorFactory = logReplicatorFactory;
    }

    public SingleClientReplicator<ID> createReplicator(ID followerId) {
        return new SingleClientReplicator<>(followerId, replicationSchedulerFactory.create(), logReplicatorFactory);
    }
}
