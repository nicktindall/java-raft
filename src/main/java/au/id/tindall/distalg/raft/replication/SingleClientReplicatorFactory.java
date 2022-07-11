package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

public class SingleClientReplicatorFactory<ID extends Serializable> {

    private final ReplicationSchedulerFactory<ID> replicationSchedulerFactory;
    private final LogReplicatorFactory<ID> logReplicatorFactory;
    private final SnapshotReplicatorFactory<ID> snapshotReplicatorFactory;

    public SingleClientReplicatorFactory(ReplicationSchedulerFactory<ID> replicationSchedulerFactory,
                                         LogReplicatorFactory<ID> logReplicatorFactory,
                                         SnapshotReplicatorFactory<ID> snapshotReplicatorFactory) {
        this.replicationSchedulerFactory = replicationSchedulerFactory;
        this.logReplicatorFactory = logReplicatorFactory;
        this.snapshotReplicatorFactory = snapshotReplicatorFactory;
    }

    public SingleClientReplicator<ID> createReplicator(ID serverId, ID followerId) {
        return new SingleClientReplicator<>(followerId, replicationSchedulerFactory.create(serverId), logReplicatorFactory, snapshotReplicatorFactory);
    }
}
