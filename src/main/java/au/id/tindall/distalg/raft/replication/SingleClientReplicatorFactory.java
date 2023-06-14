package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

public class SingleClientReplicatorFactory<I extends Serializable> {

    private final ReplicationSchedulerFactory<I> replicationSchedulerFactory;
    private final LogReplicatorFactory<I> logReplicatorFactory;
    private final SnapshotReplicatorFactory<I> snapshotReplicatorFactory;
    private final ReplicationStateFactory<I> replicationStateFactory;

    public SingleClientReplicatorFactory(ReplicationSchedulerFactory<I> replicationSchedulerFactory,
                                         LogReplicatorFactory<I> logReplicatorFactory,
                                         SnapshotReplicatorFactory<I> snapshotReplicatorFactory,
                                         ReplicationStateFactory<I> replicationStateFactory) {
        this.replicationSchedulerFactory = replicationSchedulerFactory;
        this.logReplicatorFactory = logReplicatorFactory;
        this.snapshotReplicatorFactory = snapshotReplicatorFactory;
        this.replicationStateFactory = replicationStateFactory;
    }

    public SingleClientReplicator<I> createReplicator(I serverId, I followerId, MatchIndexAdvancedListener<I> matchIndexAdvancedListener) {
        return new SingleClientReplicator<>(
                replicationSchedulerFactory.create(serverId),
                logReplicatorFactory,
                snapshotReplicatorFactory,
                replicationStateFactory.createReplicationState(followerId, matchIndexAdvancedListener));
    }
}
