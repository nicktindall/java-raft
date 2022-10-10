package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

public class SingleClientReplicatorFactory<ID extends Serializable> {

    private final ReplicationSchedulerFactory<ID> replicationSchedulerFactory;
    private final LogReplicatorFactory<ID> logReplicatorFactory;
    private final SnapshotReplicatorFactory<ID> snapshotReplicatorFactory;
    private final ReplicationStateFactory<ID> replicationStateFactory;

    public SingleClientReplicatorFactory(ReplicationSchedulerFactory<ID> replicationSchedulerFactory,
                                         LogReplicatorFactory<ID> logReplicatorFactory,
                                         SnapshotReplicatorFactory<ID> snapshotReplicatorFactory,
                                         ReplicationStateFactory<ID> replicationStateFactory) {
        this.replicationSchedulerFactory = replicationSchedulerFactory;
        this.logReplicatorFactory = logReplicatorFactory;
        this.snapshotReplicatorFactory = snapshotReplicatorFactory;
        this.replicationStateFactory = replicationStateFactory;
    }

    public SingleClientReplicator<ID> createReplicator(ID serverId, ID followerId) {
        return new SingleClientReplicator<>(
                replicationSchedulerFactory.create(serverId),
                logReplicatorFactory,
                snapshotReplicatorFactory,
                replicationStateFactory.createReplicationState(followerId));
    }
}
